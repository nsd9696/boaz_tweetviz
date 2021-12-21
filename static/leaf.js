var mymap = L.map('mapid').setView([51.512, -0.104], 1);

L.tileLayer('https://api.mapbox.com/styles/v1/{id}/tiles/{z}/{x}/{y}?access_token={accessToken}', {
    attribution: 'Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors, Imagery © <a href="https://www.mapbox.com/">Mapbox</a>',
    maxZoom: 18,
    id: 'mapbox/streets-v11',
    tileSize: 512,
    zoomOffset: -1,
    accessToken: 'pk.eyJ1IjoibnNkOTYiLCJhIjoiY2ttbDBpaHp2MDFlMzJ3cXNiMDg5MG16eiJ9.vaGHqJPUvao1560z0_TmGw'
}).addTo(mymap);

var source = new EventSource('/topic/twitterdata2');

source.addEventListener('message', function(e){
    obj = JSON.parse(e.data);
    console.log(obj);
    lat = obj.place.bounding_box.coordinates[0][0][1];
    long = obj.place.bounding_box.coordinates[0][0][0];
    username = obj.username;
    tweet = obj.text;

    marker = L.marker([lat,long], ).addTo(mymap).bindPopup('Username: <strong>' + username + '</strong><br>Tweet:  <strong>' + tweet + '</strong>');



}, false);