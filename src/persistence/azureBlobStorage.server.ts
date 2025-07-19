import * as mime from "mime";
import { ISettingsProvider } from "@paperbits/common/configuration";
import { Logger } from "@paperbits/common/logging";
import {
    AnonymousCredential,
    BlobBatch,
    BlobItem,
    BlobSASPermissions,
    BlobServiceClient,
    BlockBlobClient,
    ContainerClient
} from "@azure/storage-blob";
import { AzureBlobStorage } from "./azureBlobStorage";
import { Readable } from "stream";

/**
 * Azure blob storage client.
 */
export class ServerAzureBlobStorage extends AzureBlobStorage {
    /**
     * Creates Azure blob storage client.
     * @param storageURL Storage URL containing SAS key.
     * @param storageContainer Name of storage container.
     */
    constructor(
        protected readonly settingsProvider: ISettingsProvider,
        protected readonly logger: Logger
    ) {
        super(logger);
    }

    protected override async initContainer(): Promise<void> {
        const blobStorageBasePath = await this.settingsProvider.getSetting<string>("blobStorageBasePath") || "";
        this.basePath = this.normalizePath(blobStorageBasePath);

        const blobStorageConnectionString = await this.settingsProvider.getSetting<string>("blobStorageConnectionString");

        if (blobStorageConnectionString) {
            const containerName = await this.settingsProvider.getSetting<string>("blobStorageContainer");

            if (!containerName) {
                throw new Error(`Setting "blobStorageContainer" required to initialize AzureBlobStorage.`);
            }

            const serviceClient = BlobServiceClient.fromConnectionString(blobStorageConnectionString);

            this.containerClient = serviceClient.getContainerClient(containerName);
            this.blobBatchClient = serviceClient.getBlobBatchClient();
            return;
        }

        const blobStorageUrl = await this.settingsProvider.getSetting<string>("blobStorageUrl");

        if (blobStorageUrl) {
            this.containerClient = new ContainerClient(blobStorageUrl);
            return;
        }

        throw new Error(`Setting "blobStorageConnectionString" or "blobStorageUrl" required to initialize AzureBlobStorage.`);
    }

    /**
     * Generates download URL of a blob (without checking for its existence).
     * @param blobKey {string} Blob key.
     */
    public override async getDownloadUrl(blobKey: string): Promise<string> {
        await this.initialize();
        const blobName = this.getFullKey(blobKey);

        try {
            const blockBlobClient = this.containerClient.getBlockBlobClient(blobName);

            if (!(this.containerClient.credential instanceof AnonymousCredential) && BlobSASPermissions) {
                const now = new Date();
                now.setMinutes(now.getMinutes() - 5); // Skip clock skew with server

                const expireOn = new Date();
                expireOn.setDate(expireOn.getDate() + 1);  // temp access for 1 day   

                const sasOptions = {
                    containerName: this.containerClient.containerName,
                    startsOn: now,
                    expiresOn: expireOn,
                    blobName: blobName,
                    permissions: BlobSASPermissions.parse("r")
                };

                const blobSasUrl = await blockBlobClient.generateSasUrl(sasOptions);
                return blobSasUrl;
            }
            else {
                return blockBlobClient.url;
            }
        }
        catch (error) {
            if (error && error.statusCode && error.statusCode === 404) {
                return null; // blob was already deleted
            }
            throw error;
        }
    }

    /**
     * Removes specified blob from storage.
     * @param blobKey {string} Blob key.
     */
    public async deleteBlob(blobKey: string): Promise<void> {
        await this.initialize();

        try {
            const fullBlobKey = this.getFullKey(blobKey);
            const blockBlobClient = this.containerClient.getBlockBlobClient(fullBlobKey);
            await blockBlobClient.delete();
        }
        catch (error) {
            if (error?.statusCode === 404) {
                return; // blob was already deleted
            }
            throw error;
        }
    }

    /**
     * Get blob from storage in Browser.
     * @param blobKey {string} Blob key.
     */
    public async getBlobAsBlob(blobKey: string): Promise<Blob> {
        await this.initialize();
        const fullBlobKey = this.getFullKey(blobKey);
        const blockBlobClient = this.containerClient.getBlockBlobClient(fullBlobKey);

        const downloadBlockBlobResponse = await blockBlobClient.download();
        return downloadBlockBlobResponse.blobBody;
    }

    /**
     * Uploads specified content into storage in Node.JS
     * @param blobKey {string} Blob key.
     * @param content {ReadStream} Content stream.
     * @param contentType {string} Content type, e.g. `image/png`.
     */
    public async uploadStreamToBlob(blobKey: string, contentStream: Readable, contentType?: string): Promise<void> {
        await this.initialize();

        blobKey = this.getFullKey(blobKey);

        if (!contentType) {
            const fileName = blobKey.split("/").pop();
            contentType = mime.getType(fileName) || "application/octet-stream";
        }

        const blockBlobClient = this.containerClient.getBlockBlobClient(blobKey);


        try {
            await blockBlobClient.uploadStream(
                contentStream,
                4 * 1024 * 1024,
                20,
                {
                    blobHTTPHeaders: { blobContentType: contentType }
                }
            );
        }
        catch (error) {
            throw new Error(`Unable to upload blob ${blobKey}. ${error.stack || error.message}`);
        }
    }

    /**
     * Get blob from storage in Node.JS
     * @param blobKey {string} Blob key.
     */
    public async getBlobAsStream(blobKey: string): Promise<NodeJS.ReadableStream> {
        await this.initialize();
        const fullBlobKey = this.getFullKey(blobKey);
        const blockBlobClient = this.containerClient.getBlockBlobClient(fullBlobKey);

        const downloadBlockBlobResponse = await blockBlobClient.download();
        return downloadBlockBlobResponse.readableStreamBody;
    }

    /**
     * Get blob from storage in Node.JS
     * @param blobKey {string} Blob key.
     */
    protected async getBlobAsUint8Array(blobKey: string): Promise<Uint8Array> {
        try {
            const stream = await this.getBlobAsStream(blobKey);
            const buffer = await this.streamToBuffer(stream);
            const unit8Array = new Uint8Array(buffer.buffer);
            return unit8Array;
        }
        catch (error) {
            this.logger.trackEvent("AzureBlobStorage", { message: `Unable to download blob ${blobKey}: ${error.stack || error.message}` });
            return null;
        }
    }

    protected async streamToBuffer(readableStream: NodeJS.ReadableStream): Promise<Buffer> {
        return new Promise((resolve, reject) => {
            const chunks = [];
            readableStream.on("data", (data) => {
                chunks.push(data instanceof Buffer ? data : Buffer.from(data));
            });
            readableStream.on("end", () => {
                resolve(Buffer.concat(chunks));
            });
            readableStream.on("error", reject);
        });
    }

    /**
     * Removes blobs from storage with specified prefix.
     * if prefix is empty then all blobs from container will be removed
     * @param blobPrefix
     */
    protected async deleteBlobFolder(blobPrefix: string): Promise<void> {
        await this.initialize();

        if (!this.blobBatchClient) {
            throw new Error("deleteBlobFolder works only with client created from the blob storage connection string");
        }
        const fullBlobPrefix = this.getFullKey(blobPrefix);
        const allBlobs = await this.listAllBlobItems(fullBlobPrefix);

        try {
            const clients = allBlobs.map(item => this.containerClient.getBlockBlobClient(item.name));

            if (clients.length > 0) {
                if (clients.length < 256) {
                    await this.processDeleteBatch(clients, blobPrefix);
                }
                else {
                    const chunks = this.chunkArray(clients, 250);
                    chunks.map(async chunk => await this.processDeleteBatch(chunk, blobPrefix));
                }
            }
        }
        catch (error) {
            throw new Error(`Unable to delete blobs in ${blobPrefix}. Error: ${error}`);
        }
    }

    protected async processDeleteBatch(clients: BlockBlobClient[], blobPrefix: string) {
        const batchDeleteRequest = new BlobBatch();

        for (const client of clients) {
            await batchDeleteRequest.deleteBlob(client.url, this.containerClient.credential, {});
        }

        const result = await this.blobBatchClient.submitBatch(batchDeleteRequest, {});

        if (result.subResponsesFailedCount !== 0) {
            this.logger.trackEvent("AzureBlobStorage", { message: `Delete blob folder failed for '${blobPrefix}': ${result.subResponsesFailedCount} items.` });
        }
    }

    protected async listAllBlobItems(prefix?: string): Promise<BlobItem[]> {
        const allItems = [];

        for await (const blob of this.containerClient.listBlobsFlat({ prefix: prefix })) {
            allItems.push(blob);
        }

        return allItems;
    }

    public async createContainer(): Promise<void> {
        await this.initialize();
        await this.containerClient.createIfNotExists();
    }

    public async deleteContainer(): Promise<void> {
        await this.initialize();
        await this.containerClient.deleteIfExists();
    }

    private removeQueryParameters(url: string): string {
        return url.split('?')[0];
    }

    /**
     * Get blob from storage in Browser.
     * @param blobKey {string} Blob key.
     */
    public override async downloadBlob(blobKey: string): Promise<Uint8Array> {
        await this.initialize();
        const fullBlobKey = this.getFullKey(blobKey);
        const blockBlobClient = this.containerClient.getBlobClient(fullBlobKey);
        const blobUrl = this.removeQueryParameters(blockBlobClient.url);

        try {
            const downloadBlockBlobResponse = await blockBlobClient.download();

            if (downloadBlockBlobResponse.readableStreamBody) {
                const buffer = await this.streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
                const unit8Array = new Uint8Array(buffer.buffer);
                return unit8Array;
            }

            const statusCode = downloadBlockBlobResponse._response.status;

            this.logger.trackEvent("AzureBlobStorage", { message: `Unable to download blob ${blobUrl}. Status code: ${statusCode}` });
        }
        catch (error) {
            this.logger.trackEvent("AzureBlobStorage", { message: `Unable to download blob ${blobUrl}: ${error.stack || error.message}` });
            throw error;
        }
    }
}
