const fs = require("fs"); 
const readline = require("readline");
const path = require("path");
const {
  Worker,
  isMainThread,
  parentPort,
  workerData,
} = require("worker_threads");
const os = require("os");

const NUM_WORKERS = os.cpus().length; // Dynamically set based on CPU cores

async function processChunk(logFile, targetDate, outputFile, start, end) {
  try {
    const readStream = fs.createReadStream(logFile, {
      start,
      end,
      encoding: "utf8",
    });
    const writeStream = fs.createWriteStream(outputFile, { flags: "a" });
    const rl = readline.createInterface({
      input: readStream,
      crlfDelay: Infinity,
    });

    for await (const line of rl) {
      if (line.startsWith(targetDate)) {
        writeStream.write(line + "\n");
      }
    }
    writeStream.end();
  } catch (error) {
    console.error(`Error processing chunk: ${error.message}`);
  }
}

async function getFileSize(filePath) {
  try {
    const stats = await fs.promises.stat(filePath);
    return stats.size;
  } catch (error) {
    console.error(`Error getting file size: ${error.message}`);
    throw error;
  }
}

async function extractLogs(logFile, targetDate, outputDir = "output") {
  try {
    // Replace fs.exists with fs.access to check for existence of the outputDir
    try {
      await fs.promises.access(outputDir); // Checks if directory exists
    } catch {
      await fs.promises.mkdir(outputDir, { recursive: true }); // Creates the directory if not exists
    }

    const outputFile = path.join(outputDir, `output_${targetDate}.txt`);
    try {
      await fs.promises.access(outputFile); // Check if the file exists
      await fs.promises.unlink(outputFile); // If it exists, delete it
    } catch {} // Ignore error if file does not exist

    const fileSize = await getFileSize(logFile);
    const chunkSize = Math.ceil(fileSize / NUM_WORKERS);
    let workers = [];

    for (let i = 0; i < NUM_WORKERS; i++) {
      const start = i * chunkSize;
      const end = i === NUM_WORKERS - 1 ? fileSize : start + chunkSize - 1;

      const worker = new Worker(__filename, {
        workerData: { logFile, targetDate, outputFile, start, end },
      });

      worker.on("error", (error) => {
        console.error(`Worker error: ${error.message}`);
      });

      workers.push(worker);
    }

    await Promise.all(
      workers.map(
        (worker) => new Promise((resolve) => worker.on("exit", resolve))
      )
    );

    console.log(`✅ Logs for ${targetDate} extracted to ${outputFile}`);
  } catch (error) {
    console.error(`Error extracting logs: ${error.message}`);
  }
}

if (isMainThread) {
  if (process.argv.length !== 3) {
    console.log("Usage: node extract_logs.js YYYY-MM-DD");
    process.exit(1);
  }

  const logFile = "./logs_2024_new.log";
  const targetDate = process.argv[2];

  extractLogs(logFile, targetDate);
} else {
  const { logFile, targetDate, outputFile, start, end } = workerData;
  processChunk(logFile, targetDate, outputFile, start, end);
}
