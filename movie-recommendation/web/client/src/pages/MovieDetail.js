import React, { useEffect, useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import './MovieDetail.css';

function MovieDetail() {

    const location = useLocation();
    const clickedId = location.state.id;
    // console.log(clickedId);

    const [data, setData] = useState([{}])
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        setLoading(true);
        fetch("/get-recommendation", {
            method: "POST",
            body: JSON.stringify({id : clickedId})
        }).then(
            res => res.json()
        ).then(
            data =>{
                setData(data);
                console.log(data);
                setLoading(false);
            }
        )
    }, []);

    const productsRecommendation = [];

    for (let i = 1; i < data.length; i++){
        productsRecommendation.push(data[i]);
    }

    if (loading){
        return(
            <>
                <p>Loading...</p>
            </>
        )
    }

    return (

        <div className='container-movie-detail'>
            <div className="container-clicked-movie">
                <div className="container-img-clicked-movie">
                    <img className='poster-clicked-movie' src={data[0].Poster_Link} alt="movie poster" />
                </div>
                <div className="container-content-clicked-movie">
                    <h2>{data[0].Series_Title}</h2>
                    <table>
                        <colgroup>
                            <col style={{width:'20%'}}/>
                            <col style={{width:'80%'}}/>
                        </colgroup>
                        <tbody>
                            <tr>
                                <td>Genre</td>
                                <td>{data[0].Genre}</td>
                            </tr>
                            <tr>
                                <td>Release Year</td>
                                <td>{data[0].Released_Year}</td>
                            </tr>
                            <tr>
                                <td>Director</td>
                                <td>{data[0].Director}</td>
                            </tr>
                            <tr>
                                <td>IMDB Rating</td>
                                <td>{data[0].IMDB_Rating}</td>
                            </tr>
                            <tr>
                                <td>Synopsis</td>
                                <td>{data[0].Overview}</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            </div>
            <hr style={{width:'90%'}}/>
            <h2 className='title-recommend-movie'>Recommend Movie</h2>
            <div className="container-recommend-movie">
                {productsRecommendation.map((pr, i) => (
                    <Link onClick={()=>{window.location.reload()}} className='container-card' key={i} to="/MovieDetail" state={{id : pr.id}}>
                        <div className="movie-card">
                            <div className="container-img-movie">
                                <img className='poster' src={pr.Poster_Link} alt="movie banner" />
                            </div>
                            <div className="container-title-movie">
                                <p>{pr.Series_Title}</p>
                            </div>
                        </div>
                    </Link>
                ))}
            </div>
        </div>

        // <div>
        //     <p>clicked movie</p>
        //     <p>{data[0].id} | {data[0].Series_Title}</p>
        //     <br />
        //     <p>movie recommendation</p>
        //     {productsRecommendation.map((pr, i) => (
        //         <p key={i}>{pr.id} | {pr.Series_Title}</p>
        //     ))}
        // </div>
    )

}

export default MovieDetail