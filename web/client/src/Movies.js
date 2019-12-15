import React from 'react'
import Typography from '@material-ui/core/Typography';
import Movie from './Movie';
import { List } from '@material-ui/core';

const flexContainer = {
    display: 'flex',
    flexDirection: 'row',
    padding: 0,
  };
export default function Movies({movies}) {

    return (
        <div className="movies">
            <Typography variant="h6" component="h2">
                Recommend {movies.length} Movies
            </Typography>
            <List style={flexContainer}>
            {
                movies.slice(0, 20).map(
                    (movie, i) => <Movie key={i} movie={movie} />
                )
            }
            </List>
        </div>
    )
}