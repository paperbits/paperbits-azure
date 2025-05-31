import { expect } from "chai";
import { describe, it } from "mocha";
import { ServerAzureBlobStorage } from "../src/persistence/azureBlobStorage.server";
import { StaticSettingsProvider } from "./staticSettingsProvider";
import { ConsoleLogger } from "@paperbits/common/logging";

describe("Azure Blob Storage", async () => {
    describe("Azure Blob Storage - blobStorageUrl", async () => {
        const logger = new ConsoleLogger();

        const settingsProvider = new StaticSettingsProvider({
            "blobStorageUrl": "blob storage url with limited access"
        });
        const container = new ServerAzureBlobStorage(settingsProvider, logger);

        it("Returns list of blobs", async () => {
            const items = await container.listBlobs();
            expect(items.length).greaterThan(1);

            const url = await container.getDownloadUrl(items[0]);
            expect(url !== undefined);

            const contentBlob = await container.getBlobAsBlob(items[0]);
            expect(contentBlob === undefined);
        });

        it("Download blob", async () => {
            const items = await container.listBlobs();
            expect(items.length).greaterThan(1);

            const url = await container.getDownloadUrl(items[0]);
            expect(url !== undefined);

            const contentArray = await container.downloadBlob(items[0]);
            expect(contentArray !== undefined);
        });

        it("Returns list of blobs with prefix", async () => {
            const items = await container.listBlobs("202009162037");
            expect(items.length).greaterThan(1);
        });

        it("Returns list of blobs with prefix", async () => {
            const items = await container.listBlobs("202009162037");
            expect(items.length).greaterThan(1);
            await container.deleteBlob(items[0]);
            
            const itemsAfterDelete = await container.listBlobs("202009162037");
            expect(itemsAfterDelete.length).lessThan(items.length);
        });


    });

    describe("Azure Blob Storage - connectionString", async () => {
        const logger = new ConsoleLogger();

        const settingsProvider = new StaticSettingsProvider({
            "blobStorageContainer": "content",
            "blobStorageConnectionString": "DefaultEndpointsProtocol=https;AccountName={storage account name};AccountKey=iKlw3vTw/SuYLa4ErHD6q3GdcCg6L/qXL/RCMp8u5hhRKzlyeiokrmMpf705iQLOlYIIkF9wC4PaDTf0oLbm+A==;EndpointSuffix=core.windows.net",
        });
        const container = new ServerAzureBlobStorage(settingsProvider, logger);

        it("Returns list of blobs", async () => {
            const items = await container.listBlobs();
            expect(items.length).greaterThan(1);

            const url = await container.getDownloadUrl(items[0]);
            expect(url !== undefined);

            const contentBlob = await container.getBlobAsBlob(items[0]);
            expect(contentBlob === undefined);
        });

        it("Download blob", async () => {
            const items = await container.listBlobs();
            expect(items.length).greaterThan(1);

            const url = await container.getDownloadUrl(items[0]);
            expect(url !== undefined);

            const contentArray = await container.downloadBlob(items[0]);
            expect(contentArray !== undefined);
        });

        it("Returns list of blobs with prefix", async () => {
            const items = await container.listBlobs("202009171944");
            expect(items.length).greaterThan(1);
            await container.deleteBlob(items[0]);
            
            const itemsAfterDelete = await container.listBlobs("202009171944");
            expect(itemsAfterDelete.length).lessThan(items.length);

            // await container.deleteBlobFolder("202009171944");
            // const empty = await container.listBlobs("202009171944");
            // expect(empty.length === 0);
        });
    });
});