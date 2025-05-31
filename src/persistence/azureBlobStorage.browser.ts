import { ISettingsProvider } from "@paperbits/common/configuration";
import { Logger } from "@paperbits/common/logging";
import { ContainerClient } from "@azure/storage-blob";
import { AzureBlobStorage } from "./azureBlobStorage";

/**
 * Azure blob storage client for browser client.
 */
export class BrowserAzureBlobStorage extends AzureBlobStorage {
    /**
     * Creates Azure blob storage client.
     * @param storageURL Storage URL containing SAS key.
     * @param storageContainer Name of storage container.
     */
    constructor(
        private readonly settingsProvider: ISettingsProvider,
        protected readonly logger: Logger
    ) {
        super(logger);

        this.initializePromise = null;
        this.containerClient = null;
        this.basePath = "";
    }

    protected override async initContainer(): Promise<void> {
        const blobStorageBasePath = await this.settingsProvider.getSetting<string>("blobStorageBasePath") || "";
        this.basePath = this.normalizePath(blobStorageBasePath);

        const blobStorageUrl = await this.settingsProvider.getSetting<string>("blobStorageUrl");

        if (blobStorageUrl) {
            this.containerClient = new ContainerClient(blobStorageUrl);
            return;
        }

        throw new Error(`Setting "blobStorageConnectionString" or "blobStorageUrl" required to initialize AzureBlobStorage.`);
    }
}