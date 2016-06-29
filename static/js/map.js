var mapboxTiles = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>'
});

var colors = {
  'car': 'purple',
  'airplane': 'red',
  'subway': 'green',
  'bus': 'blue',
}

// initialize the leaflet map
var map = L.map('map')
    .addLayer(mapboxTiles)
    .setView([40.72332345541449, -73.99], 12);

map.touchZoom.disable();
map.doubleClickZoom.disable();
map.scrollWheelZoom.disable();
map.boxZoom.disable();
map.keyboard.disable();

var format = d3.time.format("%Y-%m-%d");
var date = format(new Date());

$(document).on('keypress', function(){
  var params = {};
  date = d3.time.day.offset(new Date(date), -1)
  params.date = format(date);

  $.getJSON("/transports", params, function(data) {
    collections = data.map(function(transport){
      transport.bounds = d3path.bounds(transport.geojson);
      return transport
    });
    update(collections);
  });
});

function update(data){
  // create the svg for the d3 paths
  var svgs = d3.select(map.getPanes().overlayPane)
    .selectAll('.transport')
    .data(data)
  // Exit and remove the old svgs and all the gs
  svgs.exit().remove();
  svgs.select('g').remove();
  // Update
  svgs.attr('class', function(d){
      return 'transport ' + d.type})
    .attr("width", function(d){
      return d.bounds[1][0] - d.bounds[0][0] + 120})
    .attr("height", function(d){
      return d.bounds[1][1] - d.bounds[0][1] + 120})
    .style("left", function(d){
      return d.bounds[0][0] - 50 + "px"})
    .style("top", function(d){
      return d.bounds[0][1] - 50 + "px"});

  // Enter
  svgs.enter().append("svg")
    .attr('class', function(d){
      return 'transport ' + d.type})
    .attr("width", function(d){
      return d.bounds[1][0] - d.bounds[0][0] + 120})
    .attr("height", function(d){
      return d.bounds[1][1] - d.bounds[0][1] + 120})
    .style("left", function(d){
      return d.bounds[0][0] - 50 + "px"})
    .style("top", function(d){
      return d.bounds[0][1] - 50 + "px"});

  // container elements
  var g = svgs.append("g")
    .attr("class", "transport__g")
    .attr("transform", function(d){
      return "translate(" +
        (-d.bounds[0][0] + 50) + "," + (-d.bounds[0][1] + 50) + ")"
    });

  // Points
  var ptFeatures = g.selectAll("circle")
    .data(function(d){return d.geojson.features})
  //Exit
  ptFeatures.exit().remove();
  // Enter
  ptFeatures.enter().append("circle")
    .attr("r", 3)
    .attr("class", "waypoints")

  // Update
  ptFeatures.attr("transform", function(d) {
      return "translate(" +
        applyLatLngToLayer(d).x + "," +
        applyLatLngToLayer(d).y + ")";
    });

  // Paths
  var path = g.selectAll(".lineConnect")
    .data(function(d){
      return [d.geojson.features]
    });
  // Enter
  path.enter()
    .append("path")
    .attr("class", "lineConnect");
  // Update
  path.attr("d", toLine);
  // Exit
  path.exit().remove();

}

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

function transition(path) {
  path.transition()
    .duration(7500)
    .attrTween("stroke-dasharray", function(){return tweenDash(path)})
}

function tweenDash(path) {
  return function(t) {
    //total length of path (single value)
    var l = path.node().getTotalLength();
    interpolate = d3.interpolateString("0," + l, l + "," + l);
    //t is fraction of time 0-1 since transition began
    var marker = d3.select("#marker");
    var p = path.node().getPointAtLength(t * l);
    //Move the marker to that point
    marker.attr("transform", "translate(" + p.x + "," + p.y + ")"); //move marker
    return interpolate(t);
  }
}

// Reposition the SVG to cover the features.
function update_(collections, d3path, ptFeatures, svg, path, g, toLine) {
  svg.attr("width", function(d){
      var bounds = d3path.bounds(d.geojson);
      return bounds[1][0] - bounds[0][0] + 120})
    .attr("height", function(d){
      var bounds = d3path.bounds(d.geojson);
      return bounds[1][1] - bounds[0][1] + 120})
    .style("left", function(d){
      var bounds = d3path.bounds(d.geojson);
      return bounds[0][0] - 50 + "px"})
    .style("top", function(d){
      var bounds = d3path.bounds(d.geojson);
      return bounds[0][1] - 50 + "px"});

  g.attr("transform", function(d){
    var bounds = d3path.bounds(d.geojson);
    return "translate(" + (-bounds[0][0] + 50) + "," + (-bounds[0][1] + 50) + ")"
  });

  // translate the point to the lat lng
  // datapoints are individuals features here
  ptFeatures.attr("transform", function(d) {
    return "translate(" +
      applyLatLngToLayer(d).x + "," +
      applyLatLngToLayer(d).y + ")";
  });

  // generate the line path
  path.attr("d", toLine);
}