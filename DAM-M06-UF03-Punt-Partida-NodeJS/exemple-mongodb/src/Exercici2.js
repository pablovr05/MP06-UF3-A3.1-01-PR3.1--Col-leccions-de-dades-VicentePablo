const { MongoClient } = require('mongodb');
const PDFDocument = require('pdfkit');
const fs = require('fs');
const path = require('path');
require('dotenv').config();
const log4js = require('log4js');

const logDirectory = './data/logs';
if (!fs.existsSync(logDirectory)) {
    fs.mkdirSync(logDirectory, { recursive: true });
}

log4js.configure({
    appenders: {
        file: { type: 'file', filename: path.join(logDirectory, 'exercici2.log') },
        console: { type: 'stdout' },
    },
    categories: {
        default: { appenders: ['file', 'console'], level: 'info' },
    },
});

const logger = log4js.getLogger();

const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/';
const client = new MongoClient(uri);

const outputDir = path.join(__dirname, 'data/out');
if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
}

function generatePDF(fileName, title, questions) {
    return new Promise((resolve, reject) => {
        const doc = new PDFDocument();
        const filePath = path.join(outputDir, fileName);
        const stream = fs.createWriteStream(filePath);

        doc.pipe(stream);
        doc.fontSize(18).text(title, { align: 'center' }).moveDown(1);
        doc.fontSize(12);

        if (questions.length === 0) {
            doc.text("No se encontraron preguntas.");
        } else {
            questions.forEach((q, index) => {
                doc.text(`${index + 1}. ${q.title}`).moveDown(0.5);
            });
        }

        doc.end();

        stream.on('finish', () => {
            logger.info(`📄 Archivo PDF generado: ${filePath}`);
            resolve();
        });

        stream.on('error', reject);
    });
}

async function main() {
    try {
        await client.connect();
        logger.info('✅ Conectado a MongoDB');

        const database = client.db('stackexchange_db');
        const collection = database.collection('questions');

        const avgViewCountResult = await collection.aggregate([
            { $group: { _id: null, avgViewCount: { $avg: "$viewCount" } } }
        ]).toArray();

        if (avgViewCountResult.length === 0) {
            logger.error("❌ No se pudo calcular la media de ViewCounts");
            return;
        }

        const avgViewCount = avgViewCountResult[0].avgViewCount;
        logger.info(`📊 La media de ViewCounts es: ${avgViewCount.toFixed(2)}`);

        const highViewCountQuestions = await collection.find(
            { viewCount: { $gt: avgViewCount } },
            { projection: { title: 1, _id: 0 } }
        ).toArray();

        logger.info(`🔍 Preguntas con ViewCount mayor que la media: ${highViewCountQuestions.length}`);

        const keywords = ["pug", "wig", "yak", "nap", "jig", "mug", "zap", "gag", "oaf", "elf"];
        const regex = new RegExp(keywords.join("|"), "i");

        const matchingTitleQuestions = await collection.find(
            { title: { $regex: regex } },
            { projection: { title: 1, _id: 0 } }
        ).toArray();

        logger.info(`🔍 Preguntas con títulos que contienen palabras clave: ${matchingTitleQuestions.length}`);

        await generatePDF('informe1.pdf', 'Preguntas con ViewCount mayor que la media', highViewCountQuestions);
        await generatePDF('informe2.pdf', 'Preguntas con títulos que contienen palabras clave', matchingTitleQuestions);

    } catch (error) {
        logger.error('❌ Error en la consulta a MongoDB:', error);
    } finally {
        await client.close();
        logger.info('🔌 Conexión a MongoDB cerrada');
    }
}

main();
