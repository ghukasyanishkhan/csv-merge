import { createReadStream, createWriteStream } from "fs";
import csv from "csv-parser";
import { format } from "@fast-csv/format";

// File paths - change these to match your files
const SOURCE_CSV = "source.csv"; // Contains [sku, instagramImages, designer]
const TARGET_CSV = "target.csv"; // Contains many columns including sku
const OUTPUT_CSV = "merged_output.csv"; // Will contain all columns + instagramImages + designer

// Step 1: Load all images and designers from source CSV into memory
const loadSourceData = () => {
  return new Promise((resolve, reject) => {
    const imageMap = new Map();
    const designerMap = new Map();

    createReadStream(SOURCE_CSV)
      .pipe(csv())
      .on("data", (row) => {
        const sku = row.sku.trim();
        const images = row.instagramImages || "";
        const designer = row.designer || "";
        imageMap.set(sku, images);
        designerMap.set(sku, designer);
      })
      .on("end", () => {
        console.log(`Loaded ${imageMap.size} SKU records from source CSV`);
        resolve({ imageMap, designerMap });
      })
      .on("error", (error) => {
        reject(error);
      });
  });
};

// Step 2: Process target CSV and merge with images and designers
const mergeCsvFiles = async () => {
  try {
    const { imageMap, designerMap } = await loadSourceData();
    const csvStream = format({ headers: true });
    const writableStream = createWriteStream(OUTPUT_CSV);
    csvStream.pipe(writableStream);

    let targetHeaders = [];
    let rowsProcessed = 0;

    createReadStream(TARGET_CSV)
      .pipe(csv())
      .on("headers", (headers) => {
        // Add new columns while preserving original order
        targetHeaders = [...headers, "instagramImages", "designer"];
        csvStream.write(targetHeaders); // Write headers first
      })
      .on("data", (row) => {
        const sku = row.sku.trim();
        const mergedRow = { ...row };

        // Add instagramImages and designer if SKU exists in source
        mergedRow.instagramImages = imageMap.get(sku) || "";
        mergedRow.designer = designerMap.get(sku) || "";

        // Ensure consistent column order
        const orderedRow = {};
        targetHeaders.forEach(header => {
          orderedRow[header] = mergedRow[header] || "";
        });

        csvStream.write(orderedRow);
        rowsProcessed++;
      })
      .on("end", () => {
        csvStream.end();
        console.log(`Successfully merged ${rowsProcessed} rows`);
        console.log(`Output saved to: ${OUTPUT_CSV}`);
      })
      .on("error", (error) => {
        throw error;
      });
  } catch (error) {
    console.error("Error during merge:", error);
  }
};

// Run the merge process
mergeCsvFiles();