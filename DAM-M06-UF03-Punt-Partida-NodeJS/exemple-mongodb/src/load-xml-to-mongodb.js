const fs = require('fs');
const path = require('path');
const { MongoClient } = require('mongodb');
const xml2js = require('xml2js');
require('dotenv').config();

// Ruta al fitxer XML
const xmlFilePath = path.join(__dirname, '../../data/youtubers.xml');

// Funció per llegir i analitzar el fitxer XML
async function parseXMLFile(filePath) {
  try {
    const xmlData = fs.readFileSync(filePath, 'utf-8');
    const parser = new xml2js.Parser({ 
      explicitArray: false,
      mergeAttrs: true
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
    console.error('Error llegint o analitzant el fitxer XML:', error);
    throw error;
  }
}

// Funció per processar les dades i transformar-les a un format més adequat per MongoDB
function processYoutuberData(data) {
  const youtubers = Array.isArray(data.youtubers.youtuber) 
    ? data.youtubers.youtuber 
    : [data.youtubers.youtuber];
  
  return youtubers.map(youtuber => {
    // Assegurem que categories i videos siguin arrays
    const categories = Array.isArray(youtuber.categories.category) 
      ? youtuber.categories.category 
      : [youtuber.categories.category];
    
    const videos = Array.isArray(youtuber.videos.video) 
      ? youtuber.videos.video 
      : [youtuber.videos.video];
    
    // Convertim els videos a un format més adequat
    const processedVideos = videos.map(video => ({
      videoId: video.id,
      title: video.title,
      duration: video.duration,
      views: parseInt(video.views),
      uploadDate: new Date(video.uploadDate),
      likes: parseInt(video.likes),
      comments: parseInt(video.comments)
    }));
    
    // Retornem el document processat
    return {
      youtuberId: youtuber.id,
      channel: youtuber.channel,
      name: youtuber.n,
      subscribers: parseInt(youtuber.subscribers),
      joinDate: new Date(youtuber.joinDate),
      categories: categories,
      videos: processedVideos
    };
  });
}

// Funció principal per carregar les dades a MongoDB
async function loadDataToMongoDB() {
  // Configuració de la connexió a MongoDB
  const uri = process.env.MONGODB_URI || 'mongodb://root:password@localhost:27017/';
  const client = new MongoClient(uri);
  
  try {
    await client.connect();
    console.log('Connectat a MongoDB');
    
    const database = client.db('youtubers_db');
    const collection = database.collection('youtubers');
    
    // Llegir i analitzar el fitxer XML
    console.log('Llegint el fitxer XML...');
    const xmlData = await parseXMLFile(xmlFilePath);
    
    // Processar les dades
    console.log('Processant les dades...');
    const youtubers = processYoutuberData(xmlData);
    
    // Eliminar dades existents (opcional)
    console.log('Eliminant dades existents...');
    await collection.deleteMany({});
    
    // Inserir les noves dades
    console.log('Inserint dades a MongoDB...');
    const result = await collection.insertMany(youtubers);
    
    console.log(`${result.insertedCount} documents inserits correctament.`);
    console.log('Dades carregades amb èxit!');
    
  } catch (error) {
    console.error('Error carregant les dades a MongoDB:', error);
  } finally {
    await client.close();
    console.log('Connexió a MongoDB tancada');
  }
}

// Executar la funció principal
loadDataToMongoDB();