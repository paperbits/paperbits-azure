import * as mime from "mime";
import { IBlobStorage } from "@paperbits/common/persistence";
import { Logger } from "@paperbits/common/logging";
import {
    BlobBatch,
    BlobBatchClient,
    BlobItem,
    BlockBlobClient,
    ContainerClient
} from "@azure/storage-blob";

/**
 * Azure blob storage client.
 */
export abstract class AzureBlobStorage implements IBlobStorage {
    protected initializePromise: Promise<void>;
    protected containerClient: ContainerClient;
    protected blobBatchClient: BlobBatchClient;
    protected basePath: string;

    /**
     * Creates Azure blob storage client.
     * @param storageURL Storage URL containing SAS key.
     * @param storageContainer Name of storage container.
     */
    constructor(protected readonly logger: Logger) {
        this.initializePromise = null;
        this.containerClient = null;
        this.basePath = "";
    }

    protected abstract initContainer(): Promise<void>;

    protected async initialize(): Promise<void> {
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
     * @param content
     * @param contentType
     */
    public async uploadBlob(blobKey: string, content: Uint8Array, contentType?: string): Promise<void> {
        await this.initialize();

        blobKey = this.getFullKey(blobKey);

        if (!contentType) {
            const fileName = blobKey.split("/").pop();
            contentType = mime.getType(fileName) || "application/octet-stream";
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

        if (downloadBlockBlobResponse.blobBody) {
            const blob = await downloadBlockBlobResponse.blobBody;
            const arrayBuffer = await blob.arrayBuffer();
            const unit8Array = new Uint8Array(arrayBuffer);
            return unit8Array;
        }

        throw new Error(`Unable to download blob ${blobKey}.`);
    }

    /**
     * Generates download URL of a blob (without checking for its existence).
     * @param blobKey {string} Blob key.
     */
    /**
  * Generates download URL of a blob (without checking for its existence).
  * @param blobKey {string} Blob key.
  */
    public async getDownloadUrl(blobKey: string): Promise<string> {
        await this.initialize();
        const blobName = this.getFullKey(blobKey);

        try {
            const blockBlobClient = this.containerClient.getBlockBlobClient(blobName);
            return blockBlobClient.url;

        }
        catch (error) {
            if (error && error.statusCode && error.statusCode === 404) {
                return null;
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

    protected async createContainer(): Promise<void> {
        await this.initialize();
        await this.containerClient.createIfNotExists();
    }

    protected async deleteContainer(): Promise<void> {
        await this.initialize();
        await this.containerClient.deleteIfExists();
    }

    protected normalizePath(value: string): string {
        value = value
            .replace(/\\/g, "\/")
            .replace(/\/{2,}/gm, "\/");

        if (value.startsWith("/")) {
            value = value.substring(1);
        }

        if (value.endsWith("/")) {
            value = value.slice(0, -1);
        }

        return value;
    }

    protected getFullKey(blobKey: string): string {
        const fullUrl = !!this.basePath
            ? `${this.basePath}/${blobKey}`
            : blobKey;

        return this.normalizePath(fullUrl);
    }

    /**
   * Returns an array with arrays of the given size.
   * @param srcArray {Array} Array to split.
   * @param chunkSize {Integer} Size of every group.
   */
    protected chunkArray(srcArray: any[], chunkSize: number): [][] {
        const results = [];

        while (srcArray.length) {
            results.push(srcArray.splice(0, chunkSize));
        }

        return results;
    }
}
