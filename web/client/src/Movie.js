import React from 'react'
import { ListItem } from '@material-ui/core';

const movies = require("./movies.json")

export default function Movie({ movie }) {

    const movieDetails = movies[movie]
    if (movieDetails) {
        return (
            <ListItem className='movie'>
                <div className="flex-align-mid">
                    {movieDetails.title.substr(0, 50)}
                </div>
            </ListItem>
        )
    }
    return ""
}