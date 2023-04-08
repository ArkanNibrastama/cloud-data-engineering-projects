import React, { useEffect, useState } from 'react';
import { Link } from 'react-router-dom';
import './Home.css';

function Home() {

    const [showMov, setShowMov] = useState([{}]);
    const [showLoading, setShowLoading] = useState(false);

    useEffect(() => {
        setShowLoading(true);
        fetch('/generate-movie').then(
            res => res.json()
        ).then(
            mov => {
                setShowMov(mov);
                console.log(mov);
                setShowLoading(false);
            }
        )
    }, [])

    if (showLoading){
        return(
            <p>Loading...</p>
        )
    }

    return (
        <div className="container-show-movie">
        {showMov.map((mov, i) => (
            <Link className='container-card' key={i} to="/MovieDetail" state={{id : mov.id}}>
                <div className="movie-card">
                    <div className="container-img-movie">
                        <img className='poster' src={mov.Poster_Link} alt="movie banner" />
                    </div>
                    <div className="container-title-movie">
                        <p>{mov.Series_Title}</p>
                    </div>
                </div>
            </Link>
        ))}
        </div>
    )
}

export default Home