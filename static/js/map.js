var mapboxTiles = L.tileLayer('http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    attribution: 'Map data &copy; <a href="http://openstreetmap.org">OpenStreetMap</a> contributors, <a href="http://creativecommons.org/licenses/by-sa/2.0/">CC-BY-SA</a>, Imagery © <a href="http://mapbox.com">Mapbox</a>'
});
var map = L.map('map')
    .addLayer(mapboxTiles)
    .setView([40.72332345541449, -73.99], 14);
var svg = d3.select(map.getPanes().overlayPane).append("svg");
var g = svg.append("g").attr("class", "leaflet-zoom-hide");

d3.json("/static/data/testPoint.json", function(collection) {
    var featuresdata = collection.features.filter(function(d) {
        return d.properties.id == "transport"
    });

    // INITIALIZE THE TRANSFORM GENERATOR
    var transform = d3.geo.transform({
        point: projectPoint
    });

    // INITIALIZE THE PATH GENERATOR
    var d3path = d3.geo.path().projection(transform);

    var toLine = d3.svg.line()
        .interpolate("linear")
        .x(function(d) {
            return applyLatLngToLayer(d).x
        })
        .y(function(d) {
            return applyLatLngToLayer(d).y
        });

    var ptFeatures = g.selectAll("circle")
        .data(featuresdata)
        .enter()
        .append("circle")
        .attr("r", 3)
        .attr("class", "waypoints");

    var linePath = g.selectAll(".lineConnect")
        .data([featuresdata])
        .enter()
        .append("path")
        .attr("class", "lineConnect");

    var marker = g.append("circle")
        .attr("r", 5)
        .attr("id", "marker")
        .attr("class", "travelMarker");

    var originANDdestination = [
        featuresdata[0],
        featuresdata[featuresdata.length-1]
    ]
    var begend = g.selectAll(".drinks")
        .data(originANDdestination)
        .enter()
        .append("circle", ".drinks")
        .attr("r", 5)
        .style("fill", "red")
        .style("opacity", "1");

    map.on("viewreset", reset);
    reset();
    transition();

    // Reposition the SVG to cover the features.
    function reset() {
        var bounds = d3path.bounds(collection),
            topLeft = bounds[0],
            bottomRight = bounds[1];

        begend.attr("transform",
            function(d) {
                return "translate(" +
                    applyLatLngToLayer(d).x + "," +
                    applyLatLngToLayer(d).y + ")";
            });

        ptFeatures.attr("transform",
            function(d) {
                return "translate(" +
                    applyLatLngToLayer(d).x + "," +
                    applyLatLngToLayer(d).y + ")";
            });

        // SET THE STARTING POINT
        // again, not best practice, but I'm harding coding
        // the starting point
        marker.attr("transform",
            function() {
                var y = featuresdata[0].geometry.coordinates[1]
                var x = featuresdata[0].geometry.coordinates[0]
                return "translate(" +
                    map.latLngToLayerPoint(new L.LatLng(y, x)).x + "," +
                    map.latLngToLayerPoint(new L.LatLng(y, x)).y + ")";
            });

        // Setting the size and location of the overall SVG container
        svg.attr("width", bottomRight[0] - topLeft[0] + 120)
            .attr("height", bottomRight[1] - topLeft[1] + 120)
            .style("left", topLeft[0] - 50 + "px")
            .style("top", topLeft[1] - 50 + "px");

        // linePath.attr("d", d3path);
        linePath.attr("d", toLine)
        // ptPath.attr("d", d3path);
        g.attr("transform", "translate(" + (-topLeft[0] + 50) + "," + (-topLeft[1] + 50) + ")");
    }

    function transition() {
        linePath.transition()
            .duration(7500)
            .attrTween("stroke-dasharray", tweenDash)
            .each("end", function() {
                d3.select(this).call(transition);// infinite loop
            });
    } //end transition
    // this function feeds the attrTween operator above with the
    // stroke and dash lengths
    function tweenDash() {
        return function(t) {
            //total length of path (single value)
            var l = linePath.node().getTotalLength();

            interpolate = d3.interpolateString("0," + l, l + "," + l);
            //t is fraction of time 0-1 since transition began
            var marker = d3.select("#marker");
            var p = linePath.node().getPointAtLength(t * l);
            //Move the marker to that point
            marker.attr("transform", "translate(" + p.x + "," + p.y + ")"); //move marker
            return interpolate(t);
        }
    }

    function projectPoint(x, y) {
        var point = map.latLngToLayerPoint(new L.LatLng(y, x));
        this.stream.point(point.x, point.y);
    } //end projectPoint
});

// similar to projectPoint this function converts lat/long to
// svg coordinates except that it accepts a point from our
// GeoJSON
function applyLatLngToLayer(d) {
    var y = d.geometry.coordinates[1]
    var x = d.geometry.coordinates[0]
    return map.latLngToLayerPoint(new L.LatLng(y, x))
}