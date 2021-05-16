import * as Utils from "@paperbits/common";
import { expect } from "chai";
import { describe, it } from "mocha";
import { ConsoleLogger } from "@paperbits/common/logging";
import { AzureBlobStorage } from "../src/persistence/azureBlobStorage";
import { StaticSettingsProvider } from "./staticSettingsProvider";

describe("Azure Blob Storage - blobStorageUrl", async () => {
    const logger = new ConsoleLogger();

    const settingsProvider = new StaticSettingsProvider({
        blobStorageUrl: "blob storage url with limited access",
        blobStorageContainer: "content",
        blobStorageConnectionString: "DefaultEndpointsProtocol=https;AccountName={storage account name};AccountKey={storage account key};EndpointSuffix=core.windows.net",
    });

    const container = new AzureBlobStorage(settingsProvider, logger);

    it("Returns list of blobs", async () => {
        const items = await container.listBlobs();
        expect(items.length).greaterThan(1);

        const url = await container.getDownloadUrl(items[0]);
        expect(url !== undefined);

        const content = await container.getBlobAsUint8Array(items[0]);
        expect(content !== undefined);

        const contentBlob = await container.getBlobAsBlob(items[0]);
        expect(contentBlob === undefined);
    });

    it("Download blob", async () => {
        const items = await container.listBlobs();
        expect(items.length).greaterThan(1);

        const blobKey = items[0];

        const url = await container.getDownloadUrl(blobKey);
        expect(url !== undefined);

        const content = await container.downloadBlob(blobKey);
        expect(content !== undefined);
    });

    it("Upload blob", async () => {
        const contentType = "text/plain";
        const content = Utils.stringToUnit8Array("Hello world");
        await container.uploadBlob("test/test.txt", content, contentType);
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

    it("Returns list of blobs", async () => {
        const items = await container.listBlobs();
        expect(items.length).greaterThan(1);

        const url = await container.getDownloadUrl(items[0]);
        expect(url !== undefined);

        const contentArray = await container.getBlobAsUint8Array(items[0]);
        expect(contentArray !== undefined);

        const contentBlob = await container.getBlobAsBlob(items[0]);
        expect(contentBlob === undefined);
    });


    it("Returns list of blobs with prefix", async () => {
        const items = await container.listBlobs("202009171944");
        expect(items.length).greaterThan(1);
        await container.deleteBlob(items[0]);

        const itemsAfterDelete = await container.listBlobs("202009171944");
        expect(itemsAfterDelete.length).lessThan(items.length);

        await container.deleteBlobFolder("202009171944");
        const empty = await container.listBlobs("202009171944");
        expect(empty.length === 0);
    });
});