import * as mime from "mime-types";
import { XmlHttpRequestClient } from "@paperbits/common/http";
import { ISettingsProvider } from "@paperbits/common/configuration";
import { IBlobStorage } from "@paperbits/common/persistence";
import {
    AccountSASPermissions,
    AccountSASResourceTypes,
    AccountSASServices,
    Aborter,
    BlobURL,
    BlockBlobURL,
    ContainerURL,
    ServiceURL,
    StorageURL,
    AnonymousCredential,
    SharedKeyCredential,
    SASProtocol,
    Credential,
    generateAccountSASQueryParameters
} from "@azure/storage-blob";


/**
 * Azure blob storage client.
 */
export class AzureBlobStorage implements IBlobStorage {
    private initializePromise: Promise<ContainerURL>;
    private credential: Credential;

    /**
     * Creates Azure blob storage client.
     * @param storageURL Storage URL containing SAS key.
     * @param storageContainer Name of storage container.
     */
    constructor(private readonly settingsProvider: ISettingsProvider) { }

    private async getContainerUrl(): Promise<ContainerURL> {
        const storageContainer = await this.settingsProvider.getSetting<string>("blobStorageContainer");
        const connectionString = await this.settingsProvider.getSetting<string>("blobStorageConnectionString");

        let storageUrl: string;
        let credential: Credential;

        if (connectionString) {
            const nameRegex = /AccountName=([^;]*);/gm;
            const nameMatch = nameRegex.exec(connectionString);
            const accountName = nameMatch[1];

            const keyRegex = /AccountKey=([^;]*==)/gm;
            const keyMatch = keyRegex.exec(connectionString);
            const accountKey = keyMatch[1];

            const endPoint = connectionString.split(";EndpointSuffix=");
            const endPointSuffix = endPoint.length > 1 ? endPoint[1].split(";")[0] : "core.windows.net";

            storageUrl = `https://${accountName}.blob.${endPointSuffix}`;
            credential = new SharedKeyCredential(accountName, accountKey);
        }
        else {
            storageUrl = await this.settingsProvider.getSetting<string>("blobStorageUrl");
            credential = new AnonymousCredential();
        }

        this.credential = credential;

        const pipeline = StorageURL.newPipeline(credential);
        const serviceURL: ServiceURL = new ServiceURL(storageUrl, pipeline);
        const containerURL = ContainerURL.fromServiceURL(serviceURL, storageContainer || "");

        return containerURL;
    }

    private async initialize(): Promise<ContainerURL> {
        if (!this.initializePromise) {
            this.initializePromise = this.getContainerUrl();
        }

        return this.initializePromise;
    }

    /**
     * Uploads specified content into storage.
     * @param blobKey
     * @param content
     * @param contentType
     */
    public async uploadBlob(blobKey: string, content: Uint8Array, contentType?: string): Promise<void> {
        const containerUrl = await this.initialize();

        blobKey = blobKey.replace(/\\/g, "\/").replace("//", "/");

        if (blobKey.startsWith("/")) {
            blobKey = blobKey.substring(1);
        }

        const blobURL = BlobURL.fromContainerURL(containerUrl, blobKey);
        const blockBlobURL = BlockBlobURL.fromBlobURL(blobURL);

        if (!contentType) {
            const fileName = blobKey.split("/").pop();
            contentType = mime.lookup(fileName) || "application/octet-stream";
        }

        try {
            await blockBlobURL.upload(
                Aborter.none,
                content,
                content.byteLength,
                {
                    blobHTTPHeaders: {
                        blobContentType: contentType
                    }
                }
            );
        }
        catch (error) {
            throw new Error(`Unable to upload blob ${blobKey}. ${error.stack || error.message}`);
        }
    }

    public async downloadBlob(blobKey: string): Promise<Uint8Array> {
        const httpClient = new XmlHttpRequestClient();
        const blobUrl = await this.getDownloadUrl(blobKey);
        const response = await httpClient.send({ url: blobUrl });

        if (response?.statusCode === 200) {
            return response.toByteArray();
        }

        return null;
    }

    /**
     * Generates download URL of a blob (without checking for its existence).
     * @param blobKey
     */
    public async getDownloadUrl(blobKey: string): Promise<string> {
        const containerUrl = await this.initialize();

        try {
            if (this.credential instanceof SharedKeyCredential) {
                const now = new Date();
                now.setMinutes(now.getMinutes() - 5); // Skip clock skew with server

                const tmr = new Date();
                tmr.setDate(tmr.getDate() + 1);

                const signatureValues = {
                    expiryTime: tmr,
                    permissions: AccountSASPermissions.parse("r").toString(),
                    protocol: SASProtocol.HTTPSandHTTP,
                    resourceTypes: AccountSASResourceTypes.parse("sco").toString(),
                    services: AccountSASServices.parse("btqf").toString(),
                    startTime: now,
                    version: "2016-05-31"
                };

                const sharedAccessSignature = generateAccountSASQueryParameters(signatureValues, this.credential as SharedKeyCredential).toString();
                const blobURL = BlobURL.fromContainerURL(containerUrl, blobKey);
                return `${blobURL.url}?${sharedAccessSignature}`;
            }
            else {
                const blobURL = BlobURL.fromContainerURL(containerUrl, blobKey);
                return `${blobURL.url}`;
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
     * @param blobKey
     */
    public async deleteBlob(blobKey: string): Promise<void> {
        const containerUrl = await this.initialize();

        try {
            const blobURL = BlobURL.fromContainerURL(containerUrl, blobKey);
            await blobURL.delete(Aborter.none);
        }
        catch (error) {
            if (error && error.statusCode && error.statusCode === 404) {
                return; // blob was already deleted
            }
            throw error;
        }
    }

    /**
     * Returns array of keys for all the blobs in container.
     */
    public async listBlobs?(): Promise<string[]> {
        const containerUrl = await this.initialize();

        const listBlobsResponse = await containerUrl.listBlobFlatSegment(Aborter.none, undefined);
        return listBlobsResponse.segment.blobItems.map(x => x.name);
    }

    public async createContainer(): Promise<void> {
        const containerUrl = await this.initialize();
        await containerUrl.create(Aborter.none);
    }

    public async deleteContainer(): Promise<void> {
        const containerUrl = await this.initialize();

        try {
            await containerUrl.delete(Aborter.none);
        }
        catch (error) {
            if (error && error.statusCode && error.statusCode === 404) {
                return; // container was already deleted
            }
            throw error;
        }
    }
}