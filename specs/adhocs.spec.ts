import { describe, it } from "mocha";
import { AzureBlobStorage } from "../src/persistence/azureBlobStorage";
import { StaticSettingsProvider } from "./staticSettingsProvider";


describe("AzureBlobStorage", async () => {
    it("Create container", async () => {
        const config = {
            blobStorageContainer: "$web",
            blobStorageConnectionString: ""
        };
        const settingsProvider = new StaticSettingsProvider(config);
        const azureBlobStorage = new AzureBlobStorage(settingsProvider);

       await azureBlobStorage.createContainer();
    });

    it("Delete container", async () => {
        const config = {
            blobStorageContainer: "$web",
            blobStorageConnectionString: ""
        };
        const settingsProvider = new StaticSettingsProvider(config);
        const azureBlobStorage = new AzureBlobStorage(settingsProvider);

       await azureBlobStorage.deleteContainer();
    });
});