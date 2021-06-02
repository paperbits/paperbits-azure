import { ConsoleLogger } from "@paperbits/common/logging";
import { AzureBlobStorage } from "../src/persistence/azureBlobStorage";
import { StaticSettingsProvider } from "./staticSettingsProvider";


describe("AzureBlobStorage", async () => {
    const logger = new ConsoleLogger();

    it("Create container", async () => {
        const config = {
            blobStorageContainer: "$web",
            blobStorageConnectionString: ""
        };
        const settingsProvider = new StaticSettingsProvider(config);
        const azureBlobStorage = new AzureBlobStorage(settingsProvider, logger);

        await azureBlobStorage.createContainer();
    });

    it("Delete container", async () => {
        const config = {
            blobStorageContainer: "$web",
            blobStorageConnectionString: ""
        };
        const settingsProvider = new StaticSettingsProvider(config);
        const azureBlobStorage = new AzureBlobStorage(settingsProvider, logger);

        await azureBlobStorage.deleteContainer();
    });
});