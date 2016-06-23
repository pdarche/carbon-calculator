var mapboxTiles = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>'
});

// initialize the leaflet map
var map = L.map('map')
    .addLayer(mapboxTiles)
    .setView([40.72332345541449, -73.99], 13);

d3.json("/static/data/testPoints.json", function(data) {
  // create an array of feature arrays
  collections = data.points;
  collections = collections.map(function(collection){
    collection.bounds = d3path.bounds(collection);
    return collection
  });

  // create the svg for the d3 paths
  var svg = d3.select(map.getPanes().overlayPane)
    .selectAll('.transport')
    .data(collections)
    .enter()
    .append("svg")
    .attr('class', 'transport');

  // create the container elements
  var g = svg.append("g")
    .attr("class", "transport__g");

  // create the points
  var ptFeatures = g.selectAll("circle")
    .data(function(d){return d.features})
    .enter()
    .append("circle")
    .attr("r", 3)
    .attr("class", "waypoints");

  // create the line
  var linePath = g.selectAll(".lineConnect")
    .data(function(d){return [d.features]})
    .enter()
    .append("path")
    .attr("class", "lineConnect");

  map.on("viewreset", function(ev){
    update(collections, d3path, ptFeatures, svg, linePath, g, toLine);
  });
  update(collections, d3path, ptFeatures, svg, linePath, g, toLine);

});

// transform generator
var transform = d3.geo.transform({
  point: projectPoint
});
// path generator
var d3path = d3.geo.path().projection(transform);

// svg path generator
var toLine = d3.svg.line()
  .interpolate("linear")
  .x(function(d) {
    return applyLatLngToLayer(d).x
  })
  .y(function(d) {
    return applyLatLngToLayer(d).y
  });

function projectPoint(x, y) {
  var point = map.latLngToLayerPoint(new L.LatLng(y, x));
  this.stream.point(point.x, point.y);
}

// converts lat/long to svg coordinates except from a point
function applyLatLngToLayer(d) {
  var y = d.geometry.coordinates[1];
  var x = d.geometry.coordinates[0];
  return map.latLngToLayerPoint(new L.LatLng(y, x))
}

// Reposition the SVG to cover the features.
function update(collections, d3path, ptFeatures, svg, linePath, g, toLine) {
  var bounds = d3path.bounds(collections[0])
    , topLeft = bounds[0]
    , bottomRight = bounds[1];

  svg.attr("width", function(d){
      var bounds = d3path.bounds(d);
      return bounds[1][0] - bounds[0][0] + 120})
    .attr("height", function(d){
      var bounds = d3path.bounds(d);
      return bounds[1][1] - bounds[0][1] + 120})
    .style("left", function(d){
      var bounds = d3path.bounds(d);
      return bounds[0][0] - 50 + "px"})
    .style("top", function(d){
      var bounds = d3path.bounds(d);
      return bounds[0][1] - 50 + "px"});

  g.attr("transform", function(d){
    var bounds = d3path.bounds(d);
    return "translate(" + (-bounds[0][0] + 50) + "," + (-bounds[0][1] + 50) + ")"
  });

  // translate the point to the lat lng
  ptFeatures.attr("transform", function(d) {
    return "translate(" +
      applyLatLngToLayer(d).x + "," +
      applyLatLngToLayer(d).y + ")";
  });

  // generate the line path
  linePath.attr("d", toLine);
}