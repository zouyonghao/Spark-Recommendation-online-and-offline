import React from 'react';
import './App.css';
import Typography from '@material-ui/core/Typography';

import Movies from './Movies';
import { TextField, Button } from '@material-ui/core';

// const movies = require("./movies.json")

const MOVIE_OFFLINE_API_URL = '/api/offline?userId=';
const MOVIE_NEARLINE_API_URL = '/api/nearline?userId=';
// const MOVIE_ONLINE_API_URL = '/api/online?userId=';

const MOVIE_RATE_API_URL = '/api/rate';

async function fetchOfflineMovies(update, userId) {
  console.log(userId)
  const res = await fetch(MOVIE_OFFLINE_API_URL + userId);
  let text = await res.text();
  update(text.split("|").map(i => parseInt(i)));
}
async function fetchNearlineMovies(update, userId) {
  const res = await fetch(MOVIE_NEARLINE_API_URL + userId);
  let text = await res.text();
  // console.log(text.split("|").map(i => parseInt(i.split(",")[0])))
  update(text.split("|").map(i => parseInt(i.split(",")[0])));
}
async function fetchOnlineMovies(update, userId) {
  // const res = await fetch(MOVIE_ONLINE_API_URL + userId);
  // let text = await res.text();
  console.log(Array(10).map(i => Math.floor(Math.random() * 1000)))
  update(Array.from({length: 40}, () => Math.floor(Math.random() * 1000)));
}

function App() {

  const [userId, setUserId] = React.useState("1");
  const [movieId, setMovieId] = React.useState("");
  const [rating, setRating] = React.useState("");

  const [offlineMovieList, updateOfflineMovies] = React.useState([]);
  const [nearlineMovieList, updateNearlineMovies] = React.useState([]);
  const [onlineMovieList, updateOnlineMovies] = React.useState([]);

  React.useEffect((userId) => {
    fetchOfflineMovies(updateOfflineMovies, 1);
    fetchNearlineMovies(updateNearlineMovies, 1);
    fetchOnlineMovies(updateOnlineMovies, 1);
  }, [])
  const handleChange = event => {
    setUserId(event.target.value);
    if (!event.target.value)
      return
    fetchOfflineMovies(updateOfflineMovies, event.target.value);
    fetchNearlineMovies(updateNearlineMovies, event.target.value);
    fetchOnlineMovies(updateOnlineMovies, event.target.value);
  };

  const handleMovieId = event => {
    setMovieId(event.target.value);
  };
  const handleRating = event => {
    setRating(event.target.value);
  };

  const rateMovie = event => {
    fetch(MOVIE_RATE_API_URL + "?userId=" + userId + "&movieId=" + movieId + "&rating="+rating, {
      method: "POST"
    });
  };

  return (
    <div className="App">
      <TextField id="userId" label="userId" value={userId} onChange={handleChange} />
      <br />
      <TextField id="movieId" label="movieId" value={movieId} onChange={handleMovieId} />
      <TextField id="rating" label="rating" value={rating} onChange={handleRating} />
      <Button color="primary" onClick={rateMovie}>Rate a movie!</Button>
      <Typography variant="h4" component="h1">
        OnlineRecommendation
      </Typography>
      <Movies movies={onlineMovieList} />
      <Typography variant="h4" component="h1">
        NearLineRecommendation
      </Typography>
      <Movies movies={nearlineMovieList} />
      <Typography variant="h4" component="h1">
        OfflineRecommendation
      </Typography>
      <Movies movies={offlineMovieList} />
    </div>
  );
}

export default App;
