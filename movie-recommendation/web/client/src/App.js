import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import Home from './pages/Home';
import MovieDetail from './pages/MovieDetail';

function App() {
  
  return (
    
    <>
      <Router>
        <Routes>
          <Route path='/' exact element={<Home/>} />
          <Route path='/MovieDetail' element={<MovieDetail/>} />
        </Routes>
      </Router>
    </>

  );
}

export default App;
