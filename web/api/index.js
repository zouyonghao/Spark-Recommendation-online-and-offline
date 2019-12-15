const express = require('express')
const app = express()
const port = 3001

var redis = require("redis"),
    client = redis.createClient({ host: "219.223.181.246" });

const { promisify } = require('util');
const getAsync = promisify(client.get).bind(client);

app.get('/api/offline', async (req, res) => {
    console.log(req.query)
    const recommendMovies = await getAsync(req.query.userId + "_offline");
    res.header("Access-Control-Allow-Origin", "*");
    return res.send(recommendMovies)
})

app.get('/api/nearline', async (req, res) => {
    console.log(req.query)
    const recommendMovies = await getAsync(req.query.userId);
    res.header("Access-Control-Allow-Origin", "*");
    return res.send(recommendMovies)
})

app.get('/api/online', async (req, res) => {
    console.log(req.query)
    const recommendMovies = await getAsync(req.query.userId);
    res.header("Access-Control-Allow-Origin", "*");
    return res.send(recommendMovies)
})
var net = require('net');

var socketClient = net.createConnection({ host: "localhost", port: 4321 });
socketClient.on('data', function (data) {
    console.log('Server return data : ' + data);
});

app.post('/api/rate', async (req, res) => {
    console.log(req.query)
    const userId = req.query.userId;
    const movieId = req.query.movieId;
    const rating = req.query.rating;
    console.log(userId + " " + movieId + " " + rating)
    socketClient.write(userId + " " + movieId + " " + rating + "\n")
    // socketClient.write("")
    res.header("Access-Control-Allow-Origin", "*");
    return res.send("success")
})

app.listen(port, () => console.log(`Example app listening on port ${port}!`))