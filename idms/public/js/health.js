
/*'	    <div class="two wide column">
	      <h4 class="ui padded header">ferry00</h4>
	    </div>
	    <div class="fourteen wide column">
	      <svg height="30" width="100%">
		<rect x="0" y="10" height="10" width="80%" fill="black"/>
		<rect x="80%" y="10" height="10" width="20%" fill="red"/>
	      </svg>
	    </div>
'*/

function findGetParameter(parameterName) {
    var result = null,
        tmp = [];
    location.search.substr(1).split("&").forEach(function (item) {
          tmp = item.split("=");
        if (tmp[0] === parameterName) {
	    result = tmp[1];
	}
    });
    return result;
}

function SVG(tag) {
    return document.createElementNS('http://www.w3.org/2000/svg', tag);
}

$(document).ready(function() {
    let fid = findGetParameter("id");
    $.get("h/" + fid,
	  function(data) {
	      let mag = ["B", "KB", "MB", "GB", "TB", "PB"];
	      let size = data['rawsize'];
	      var mag_level = 0;
	      while (data['rawsize'] > 1024) {
		  mag_level++;
		  data['rawsize'] /= 1024;
	      }
	      $("#lblSize").html(data['rawsize'] + mag[mag_level]);
	      $("#hName").html(data['filename']).removeClass("placeholder");
	      data['depots'].forEach((depot) => {
		  let ac = (depot['alive'] ? "rgb(33,186,69)" : "rgb(232,232,232)");
		  let dc = (depot['alive'] ? "rgb(251,189,8)" : "rgb(232,232,232)");
		  let _depot = $(`
                    <div class="two wide column">
                      <h4 class="ui padded ${depot['alive'] ? "" : "disabled"} header">${depot['depot']}</h4>
                    </div>
                    <div class="fourteen wide column">
                      <svg height="30" width="100%">
                      </svg>
                    </div>
                  `);
		  let svg = _depot.find("svg");
		  depot['allocs']['dead'].forEach((alloc) => {
		      let offset = Math.floor((alloc[0] / size) * 100);
		      let width = Math.floor((alloc[1] / size) * 100);
		      let rect = $(SVG('rect'))
			  .attr("x", offset + "%")
			  .attr("y", "10")
			  .attr("width", width + "%")
			  .attr("height", "10")
			  .attr("fill", dc)
 			  .attr("data-type", "dead")
			  .appendTo(svg);
		  });
		  depot['allocs']['alive'].forEach((alloc) => {
		      let offset = Math.floor((alloc[0] / size) * 100);
		      let width = Math.floor((alloc[1] / size) * 100);
		      let rect = $(SVG('rect'))
			  .attr("x", offset + "%")
			  .attr("y", "10")
			  .attr("width", width + "%")
			  .attr("height", "10")
			  .attr("fill", ac)
 			  .attr("data-type", "alive")
			  .appendTo(svg);
		  });
		  $("#divGrid").append(_depot);
	      });
	      console.log(data);
	  });
});

