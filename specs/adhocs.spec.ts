import { describe, it } from "mocha";
import { StaticSettingsProvider } from "./staticSettingsProvider";
import { ServerAzureBlobStorage } from "../src/persistence/azureBlobStorage.server";
import { ConsoleLogger } from "@paperbits/common/logging";


describe("AzureBlobStorage", async () => {
    const logger = new ConsoleLogger();

    it("Create container", async () => {
        const config = {
            blobStorageContainer: "$web",
            blobStorageConnectionString: ""
        };
        const settingsProvider = new StaticSettingsProvider(config);
        const azureBlobStorage = new ServerAzureBlobStorage(settingsProvider, logger);

        await azureBlobStorage.createContainer();
    });

    it("Delete container", async () => {
        const config = {
            blobStorageContainer: "$web",
            blobStorageConnectionString: ""
        };
        const settingsProvider = new StaticSettingsProvider(config);
        const azureBlobStorage = new ServerAzureBlobStorage(settingsProvider, logger);

        await azureBlobStorage.deleteContainer();
    });
});