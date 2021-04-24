import * as mime from "mime-types";
import { ReadStream } from "fs";
import { ISettingsProvider } from "@paperbits/common/configuration";
import { IBlobStorage } from "@paperbits/common/persistence";
import { Logger } from "@paperbits/common/logging";
import {
    AnonymousCredential,
    BlobBatch,
    BlobBatchClient,
    BlobItem,
    BlobSASPermissions,
    BlobServiceClient,
    BlockBlobClient,
    ContainerClient
} from "@azure/storage-blob";

/**
 * Azure blob storage client.
 */
export class AzureBlobStorage implements IBlobStorage {
    private initializePromise: Promise<void>;
    private blobBatchClient: BlobBatchClient;
    private containerClient: ContainerClient;
    private basePath: string;

    /**
     * Creates Azure blob storage client.
     * @param storageURL Storage URL containing SAS key.
     * @param storageContainer Name of storage container.
     */
    constructor(
        private readonly settingsProvider: ISettingsProvider,
        private readonly logger: Logger
    ) { }

    private async initContainer(): Promise<void> {
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

    private async initialize(): Promise<void> {
        if (!this.initializePromise) {
            this.initializePromise = this.initContainer();
        }

        return this.initializePromise;
    }

    /**
     * Returns array of keys for all the blobs in container or with specified prefix.
     * @param blobPrefix {string} Blob prefix.
     */
    public async listBlobs(blobPrefix: string = ""): Promise<string[]> {
        await this.initialize();
        const prefix = this.getFullKey(blobPrefix);
        const allBlobs = await this.listAllBlobItems(prefix);

        if (allBlobs.length > 0) {
            return allBlobs.map(blob => blob.name);
        }

        return [];
    }

    /**
     * Uploads specified content into storage.
     * @param blobKey {string} Blob key.
     * @param content {Uint8Array} Blob content.
     * @param contentType {sting} Blob content type.
     */
    public async uploadBlob(blobKey: string, content: Uint8Array, contentType?: string): Promise<void> {
        await this.initialize();

        blobKey = this.getFullKey(blobKey);

        if (!contentType) {
            const fileName = blobKey.split("/").pop();
            contentType = mime.lookup(fileName) || "application/octet-stream";
        }

        const blockBlobClient = this.containerClient.getBlockBlobClient(blobKey);
        try {
            await blockBlobClient.upload(
                content,
                content.byteLength,
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
     * Get blob from storage in Browser.
     * @param blobKey {string} Blob key.
     */
    public async downloadBlob(blobKey: string): Promise<Uint8Array> {
        await this.initialize();
        const fullBlobKey = this.getFullKey(blobKey);
        const blockBlobClient = this.containerClient.getBlobClient(fullBlobKey);
        const downloadBlockBlobResponse = await blockBlobClient.download();

        // if browser
        if (downloadBlockBlobResponse.blobBody) {
            const blob = await downloadBlockBlobResponse.blobBody;
            const arrayBuffer = await blob.arrayBuffer();
            const unit8Array = new Uint8Array(arrayBuffer);
            return unit8Array;
        }

        // if Node JS
        if (downloadBlockBlobResponse.readableStreamBody) {
            const buffer = await this.streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
            const unit8Array = new Uint8Array(buffer.buffer);
            return unit8Array;
        }

        throw new Error(`Unable to download blob ${blobKey}.`);
    }

    /**
     * Generates download URL of a blob (without checking for its existence).
     * @param blobKey {string} Blob key.
     */
    public async getDownloadUrl(blobKey: string): Promise<string> {
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
    public async uploadStreamToBlob(blobKey: string, contentStream: ReadStream, contentType?: string): Promise<void> {
        await this.initialize();

        blobKey = this.getFullKey(blobKey);

        if (!contentType) {
            const fileName = blobKey.split("/").pop();
            contentType = mime.lookup(fileName) || "application/octet-stream";
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
    public async getBlobAsUint8Array(blobKey: string): Promise<Uint8Array> {
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

    private async streamToBuffer(readableStream: NodeJS.ReadableStream): Promise<Buffer> {
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
    public async deleteBlobFolder(blobPrefix: string): Promise<void> {
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

    private async processDeleteBatch(clients: BlockBlobClient[], blobPrefix: string) {
        const batchDeleteRequest = new BlobBatch();

        for (const client of clients) {
            await batchDeleteRequest.deleteBlob(client.url, this.containerClient.credential, {});
        }

        const result = await this.blobBatchClient.submitBatch(batchDeleteRequest, {});

        if (result.subResponsesFailedCount !== 0) {
            this.logger.trackEvent("AzureBlobStorage", { message: `Delete blob folder failed for '${blobPrefix}': ${result.subResponsesFailedCount} items.` });
        }
    }

    private async listAllBlobItems(prefix?: string): Promise<BlobItem[]> {
        const allItems = [];

        for await (const blob of this.containerClient.listBlobsFlat({ prefix: prefix })) {
            allItems.push(blob);
        }

        return allItems;
    }

    /**
     * Creates blob container in Azure Storage service.
     */
    public async createContainer(): Promise<void> {
        await this.initialize();
        await this.containerClient.createIfNotExists();
    }

    /**
     * Deletes blob container from Azure Storage service.
     */
    public async deleteContainer(): Promise<void> {
        await this.initialize();
        await this.containerClient.deleteIfExists();
    }

    private normalizePath(value: string): string {
        value = value.replace(/\\/g, "\/");

        if (value.startsWith("/")) {
            value = value.substring(1);
        }

        if (value.endsWith("/")) {
            value = value.slice(0, -1);
        }

        return value.replace(/\/{2,}/gm, "\/");
    }

    private getFullKey(blobKey: string): string {
        return `${this.basePath}/${this.normalizePath(blobKey)}`;
    }

    /**
     * Returns an array with arrays of the given size.
     * @param srcArray {Array} Array to split.
     * @param chunkSize {Integer} Size of every group.
     */
    public chunkArray(srcArray: any[], chunkSize: number): [][] {
        const results = [];

        while (srcArray.length) {
            results.push(srcArray.splice(0, chunkSize));
        }

        return results;
    }
}