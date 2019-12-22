import React from 'react'
import Async from 'react-async'
import { ListItem } from '@material-ui/core';

const movies = require("./movies.json")
const links = require("./links.json")

const omdbAPI = "http://www.omdbapi.com/?apikey=24e65b04&i=tt"

const loadData = ({imdbID}) =>
    fetch(omdbAPI + imdbID)
        .then(res => (res.ok ? res : Promise.reject(res)))
        .then(res => res.json())

export default function Movie({ movie }) {

    const movieDetails = movies[movie]
    if (movieDetails) {
        return (
            <ListItem className='movie'>
                <div>
                    <Async promiseFn={loadData} imdbID={links[movie]}>
                        {({ data }) => {
                            if (data)
                                return (<img src={data.Poster} width="100" alt={movieDetails.title.substr(0, 50)}/>)
                        }
                        }
                    </Async>
                </div>
            </ListItem>
        )
    }
    return ""
}