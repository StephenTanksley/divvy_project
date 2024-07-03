import { useState, useEffect } from 'react';
import './App.css';
import axios from 'axios';

let params = {
    date: '2020-05-01',
    time_start: 0,
    time_end: 24,
    min_lat: 41.66,
    max_lat: 42.16,
    min_lng: -87.87,
    max_lng: -87.53
}


function App() {
  let [trips, setTrips] = useState([]);
  useEffect(() => {
    axios.get('http://localhost:5000/trips', params).then(response => {
      setTrips(response.data);
    }).catch(e => {
      console.log("An error occurred: ", e);
    })
  }, [])

  let renderListOfTrips = (trips) => {
    return trips.map(trip => 
    <div className='containerItem' key={trip.id}>
        <h3>{trip.id}</h3>
        <h4>{trip.start_station_id}</h4>
      </div>)
  }

  let smallSection = trips.slice(0, 100);
  return (
    <div className="App">
      <div>
        <h2>This is an example component</h2>
        <div className='containerDiv'>
          {renderListOfTrips(smallSection)}
        </div>
      </div>
    </div>
  );  
}

export default App;
