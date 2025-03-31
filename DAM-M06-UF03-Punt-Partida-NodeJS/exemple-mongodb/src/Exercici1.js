const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
const htmlEntities = require('html-entities').AllHtmlEntities;
const log4js = require('log4js');
require('dotenv').config();

const xmlFilePath = path.join(__dirname, 'academia.stackexchange.com/Posts.xml');

const logDirectory = './data/logs';
if (!fs.existsSync(logDirectory)) {
  fs.mkdirSync(logDirectory, { recursive: true });
}

log4js.configure({
  appenders: {
    file: {
      type: 'file',
      filename: path.join(logDirectory, 'exercici1.log'),
    },
    console: {
      type: 'stdout',
    },
  },
  categories: {
    default: { appenders: ['file', 'console'], level: 'info' },
  },
});

const logger = log4js.getLogger();

async function parseXMLFile(filePath) {
  try {
    const xmlData = fs.readFileSync(filePath, 'utf-8');
    const parser = new xml2js.Parser({
      explicitArray: false,
      mergeAttrs: true,
    });

    return new Promise((resolve, reject) => {
      parser.parseString(xmlData, (err, result) => {
        if (err) {
          reject(err);
        } else {
          resolve(result);
        }
      });
    });
  } catch (error) {
    logger.error('Error llegint o analitzant el fitxer XML:', error);
    throw error;
  }
}

function processPostsData(data) {
    console.log('Datos XML:', data);
    const posts = Array.isArray(data.posts.row) 
      ? data.posts.row
      : [data.posts.row];
    
    return posts.map(post => {
      if (post.ParentId) {
        console.error('El post es un subPost, esquivant...');
      }

      return {
        postId: post.Id,
        title: post.Title,
        body: post.Body,
        tags: post.Tags,
        score: parseInt(post.Score),
        viewCount: post.ViewCount ? parseInt(post.ViewCount) : 0,
        answerCount: parseInt(post.AnswerCount),
        commentCount: parseInt(post.CommentCount),
        creationDate: new Date(post.CreationDate),
        lastActivityDate: new Date(post.LastActivityDate),
        contentLicense: post.ContentLicense,
        ownerUserId: post.OwnerUserId,
        acceptedAnswerId: post.AcceptedAnswerId,
      };
    });
}

async function loadDataToMongoDB() {
    const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017/';
    const client = new MongoClient(uri);

    try {
      await client.connect();
      logger.info('Connectat a MongoDB');
  
      const database = client.db('stackexchange_db');
      const collection = database.collection('questions');

      logger.info('Llegint el fitxer XML...');
      const xmlData = await parseXMLFile(xmlFilePath);

      logger.info('Processant les dades...');
      const questions = processPostsData(xmlData);
  
      logger.info(`Total de preguntas procesadas: ${questions.length}`);

      const sortedQuestions = questions
        .sort((a, b) => b.viewCount - a.viewCount)
        .slice(0, 10000);
  
      logger.info('Eliminant dades existents...');
      await collection.deleteMany({});
  
      logger.info('Inserint dades a MongoDB...');
      const result = await collection.insertMany(sortedQuestions);
  
      logger.info(`${result.insertedCount} documents inserits correctament.`);
      logger.info('Dades carregades amb èxit!');
    } catch (error) {
      logger.error('Error carregant les dades a MongoDB:', error);
    } finally {
      await client.close();
      logger.info('Connexió a MongoDB tancada');
    }
  }

loadDataToMongoDB();
