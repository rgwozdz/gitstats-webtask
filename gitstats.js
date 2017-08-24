'use latest';

import express from 'express';
import { fromExpress } from 'webtask-tools';
import bodyParser from 'body-parser';
import { MongoClient } from 'mongodb';
import { ObjectID } from 'mongodb';
import multer from 'multer';
import path from 'path';

const app = express();
const storage = multer.memoryStorage();
const upload = multer({ storage: storage });
// Define Mongo Collection Name
const collection = 'commits';

app.use(bodyParser.json());


app.get('/:author/commits-by-repo', (req, res, next) => {
  var db;
  const { MONGO_URL } = req.webtaskContext.data;
  const author  = req.params.author;
    MongoClient.connect(MONGO_URL)
    .then(dbConn=>{
      db = dbConn;
      return db.collection(collection).aggregate([{ $match: { author: author}},{$group : { _id : "$repo", count: { $sum: 1 } }},
      { $sort : {count : -1}} ]).toArray();
    })
    .then(result=>{
      var entries = result.map(item=>{
        return {repoName: item._id, lifetimeCommits: item.count}
      })
      res.set('Content-Type', 'application/json');
      res.status(200).json({author: author, commits: entries});
      db.close();
      
    })
    .catch(err=>{
      next(err);
    });
});

app.get('/:author/language-stats', (req, res, next) => {
  
  var db;
  const { MONGO_URL } = req.webtaskContext.data;
  const author  = req.params.author;
    MongoClient.connect(MONGO_URL)
    .then(dbConn=>{
      db = dbConn;

      return db.collection(collection).aggregate([{ $match: { author: author}},
        {$group : { _id : "$fileExtenstion", additions: { $sum: "$additions" }, deletions: { $sum: "$deletions" } } },
        { $sort : {additions : -1} }]).toArray();
    })
    .then(result=>{
      var entries = result.map(item=>{
        return {fileType: item._id, additions: item.additions, deletions: item.deletions}
      });
      
      res.set('Content-Type', 'application/json');
      res.status(200).json({author: author, stats: entries});
      db.close();
      
    })
    .catch(err=>{
      next(err);
    });
});

app.post('/', upload.single('log'), (req, res, next) => {
  const { MONGO_URL } = req.webtaskContext.data;
  // Read File Buffer into string then convert to array of lines
  const uglyArray = req.file.buffer.toString().split('\n');
  let recArray = [];
  let timestamp;
  let date;
  let commitHash;
  
  // loop through file lines and parse
  uglyArray.forEach(item=>{
    
    if(item.match(/(\d{4})-(\d{2})-(\d{2})T(\d{2})\:(\d{2})\:(\d{2})[+-](\d{2})\:(\d{2}),\b[0-9a-f]{5,40}\b/)) {
      // Parse the date,hash line
      let itemArr = item.split(',');
      timestamp = itemArr[0];
      commitHash = itemArr[1];
      date = timestamp.split('T')[0];
    } else if(item !== "") {
      
      // Parse the --num-stats line; 
      let fileLineParse = item.trim().replace(/  +/g, ",").replace(/\t/g, ",").replace('{', "").replace('}', "").split(',');
      
      // Exlude binary file stats
      if(fileLineParse[0] !== '-' && fileLineParse[1] !== "-") {
        // Get the file extension
        let ext = path.extname(fileLineParse[2]).replace(".", "") === '' ? 'other' : path.extname(fileLineParse[2]).replace(".", "");
        
        // Add to a record array for Mongo
        recArray.push({author: req.body.user, repo: req.body.repo, commit: commitHash, timestamp: timestamp, date: date, additions:Number(fileLineParse[0]), deletions: Number(fileLineParse[1]), fileExtenstion: ext });
      }
    }
  });

  // Write to MongoDB
  var db;
  MongoClient.connect(MONGO_URL)
    .then(dbConn=>{
      db = dbConn;
      // Delete all old repo records to avoid duplication
      return db.collection(collection).deleteMany({repo: req.body.repo });
    })
    .then(result=>{
      return db.collection(collection).insertMany(recArray);
    })
    .then(result=>{
      db.close();
      res.status(200).json(result);
    })
    .catch(err=>{
      next(err);
    });

});

module.exports = fromExpress(app);
