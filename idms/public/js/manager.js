var models = {}
var policies = new XMLHttpRequest();
var pending_drop;
var collapsed_folders = []

$(window).click(function() {
    $("#adownload").parent().hide();
});

function generate_policy() {
    function generate_match(root) {
	let ty = root.data("type");
	let valid = true;
	result = {}
	if (ty == "pred") {
	    let prop = root.find(".property").val();
	    let cmp = root.find("select").val();
	    let val = root.find(".value").val();
	    
	    if (prop == '') {
		root.find(".property").addClass("idmswarn");
		valid = false;
	    }
	    if (val == '') {
		root.find(".value").addClass("idmswarn");
		valid = false;
	    }
	    if (!valid) { return false; }
	    else {
		let r = {};
		r[cmp] = val;
		result[prop] = r;
		return result;
	    }
	}
	else {
	    let ls = [];
	    root.find(".dropcontainer").first().children().each((k,v) => {
		r = generate_match($(v).find(".predicate").addBack(".predicate").first());
		valid = valid && r;
		ls.push(r);
	    });
	    if (!valid) { return false; }
	    else {
		result = {};
		result[ty] = ls;
		return result;
	    }
	}
    }
    function generate_action(root) {
	let ty = root.data("type");
	let dom_args = root.find(".hideable").first().children();
	let args = {};
	let valid = true;

	dom_args.each((k,v) => {
	    v = $(v);
	    let name = v.find("label").first().html();
	    if (v.children().hasClass("idms_list")) {
		let result = [];
		v.find(".dropcontainer").first().children().each((k,v) => {
		    r = generate_action($(v));
		    valid = valid && r;
		    result.push(r);
		});
		args[name] = result;
	    }
	    else {
		args[name] = v.find("input").val();
		if (args[name] == '') {
		    valid = false;
		    v.find("label").addClass("orange");
		}
		if (v.find("input").attr("type") == 'number') {
		    args[name] = parseInt(args[name]);
		}
	    }
	});

	return (valid ? { "$type": ty, "$args": args } : false)
    }
    let root = $("#policyroot");
    let policy = { "$match": generate_match(root.find(".predicate").first()),
		   "$action": generate_action(root.find(".policy").first())};

    if (!policy["$action"] || !policy["$match"]) {
	return;
    }

    $.post({
	url: 'r',
	data: JSON.stringify(policy),
	success: (data, status, hdr) => {
	    $.get({
		url: 'a',
		dataType: 'json',
		complete: (hdr, status) => {
		    pending_drop.reset();
		}
	    });
	}
    });
}

function generate_template(model) {
    let params = '';
    let ty = '';
    for (arg in model.args) {
	if (model.args[arg].type == 'policylist') {
	    params += `
              <div class="field">
                <div class="idms_list">
                  <label class="ui tiny header" style="display: none">${arg}</label>
	          <div class="template">
	            <div class="ui placeholder segment" style="min-height: 150px; width: 300px;">
	              <div class="ui one column grid">
	                <div class="middle aligned row">
	                  <div class="column">
		            <div class="ui center aligned icon header">
		              <i class="code icon"></i>Drag Policy type from left
		            </div>
		          </div>
		        </div>
	              </div>
	            </div>
	          </div>
                </div>
              </div>`;
	}
	else {
	    if (model.args[arg].type == 'str') {
		ty = 'text';
	    }
	    else if (model.args[arg].type == 'int') {
		ty = 'number';
	    }
	    params += `
              <div class="field">
                <div class="ui labeled mini input">
                  <label class="ui label">${arg}</label>
	          <input type="${ty}" value="${model.args[arg].default}" id="dst">
	        </div>
	      </div>\n`;
	}
    }
    let template = $(`
      <div class="ui basic segment policy" data-type="${model.tag}">
        <div class="ui medium header hidebutton">${model.name}
	  <div class="sub header">Parameters</div>
	</div>
        <div class="hideable">
	  ${params}
        </div>
      </div>`);

    return template;
}

function recur_attach(drop, model, ty) {
    model.find(".hidebutton").click(event => {
	model.find(".hideable").first().toggle()
    });
    if (model.find(".idms_list").length > 0)
	drop.attach_child(DropArea.build(model.find(".idms_list"), ty, true, recur_attach));
    return model;
}

function make_pending_policy(drop, model) {
    let template = $(`
      <div class="ui green segment policyroot">
        <form id="policyroot" class="ui form">
          <div class="ui two column padded grid">
            <div class="ui vertical divider">On</div>
            <div class="column pcontainer"></div>
            <div class="column">
              <div class="ui basic left floated segment">
                <div class="ui segments idms_predicate">
	          <div class="template">
	            <div class="ui placeholder segment" style="min-height: 150px; width: 300px;">
	              <div class="ui one column grid">
	         	<div class="middle aligned row">
		          <div class="column">
		            <div class="ui center aligned icon header">
		              <i class="code icon"></i>Drag and Drop Match or File
		            </div>
		          </div>
		        </div>
	              </div>
	            </div>
	          </div>
                </div>
              </div>
              <div class="ui basic right floated segment" style="padding: 0px">
                <div class="ui buttons">
                  <div class="ui top aligned button idms_discard">Discard</div>
                  <div class="ui top aligned primary button idms_add">Add</div>
                </div>
              </div>
            </div>
          </div>
        </form>
      </div>`);

    template.find(".pcontainer").append(model);
    template.find(".idms_discard").click(function() {
	pending_drop.reset();
    });
    template.find(".idms_add").click(generate_policy);
    let match = DropArea.build($("#pending-box-outer"), 'match', false, recur_attach, template.find(".idms_predicate"));
    let recur = recur_attach(drop, model, 'policy');
    return template;
}

function fill_toolbox() {
    for (v in models) {
	let cap = v.substring(1)[0].toUpperCase() + v.substring(1).substring(1);
	models[v].name = cap;
	models[v].tag = v;
	let template = $(`
          <div data-model="${v}" data-type="policy" draggable="true" title="${models[v].description}">
            <div class="item">
              <div class="ui secondary raised segment">
                <div class="ui tiny header">${cap}</div>
              </div>
            </div>
            <div class="template"></div>
          </div>`);
	template.find('.template').first().append(generate_template(models[v]));
	$('#toolbox-policies').append(template);
	template.on("drag", function(event) {
	    DropArea.set_target($(this));
	});
    }
}

function manual_upload(event) {
    function upload_complete() {
	console.log("Complete");
    }
    let input = $("input[type=file]")[0];
    let payload = new FormData();
    $("#filebrowser").show();
    if (input.files.length) {
	$.each(input.files, (i, f) => { payload.append('files', f); });
	$.ajax({
	    url: 'f',
	    type: 'POST',
	    data: payload,
	    dataType: 'json',
	    contentType: false,
	    processData: false,
	    error: show_filebrowser,
	    success: create_filebrowser,
	});
    }
}

function show_filebrowser(xhr, status) {
    $("#filebrowser").fadeOut('slow');
}

function create_filebrowser(data,status,xhr) {
    function _template(v) {
	return $(`
              <div class="template">
                <div class="ui horizontal segments predicate" data-type="pred">
                  <div class="segment">
                    <div class="ui mini input">
                      <input type="text" class="property" value="id" disabled>
                    </div>
                  </div>
                  <div class="segment">
                    <select style="font-size: 0.79em" disabled>
                      <option value="$eq">=</option>
                      <option value="$gt">&gt;</option>
                      <option value="$gte">&gt;=</option>
                      <option value="$lt">&lt;</option>
                      <option value="$lte">&lt;=</option>
                      <option value="$regex">regex</option>
                    </select>
                  </div>
                  <div class="segment">
                    <div class="ui mini input">
                      <input type="text" class="value" value="${v['id']}" disabled>
                    </div>
                  </div>
                </div>
              </div>
            `);
    }
    function make_cb(target) {
	function move_file(drop, model, ty) {
	    let payload = { file: DropArea.get_target().data('id'), target: target.data('id') }
	    $("#filebrowser").show();
	    $.ajax({
	      url: 'f',
	      type: 'PUT',
	      data: JSON.stringify(payload),
	      dataType: 'json',
	      contentType: false,
	      processData: false,
	      error: show_filebrowser,
	    success: create_filebrowser,
	    });
	}
	return move_file;
    }
    function _item(i, v) {
	let template = $(`
          <div class="item idms_file ${(v['mode'] == "file" ? 'idms_match' : '')}" draggable="true" data-type="file match" data-id="${v['id']}">
            <i class="${(v['mode'] == "file" ? "file" : "teal folder")} icon"></i>
            <div class="content">
              <div class="hidebutton header">${v['name']}</div>
              ${(v['mode'] != "file" ? "<div class='hideable list' style='padding: 0px;'></div><div class='template'><div style='min-height: 10px; min-width: 200px'></div></div>" : "")}
            </div>
          </div>
        `);
	if (v['mode'] == 'file') {
	    template.append(_template(v));
	    template.on('contextmenu', function(event) {
		btn_dl = $("#adownload");
		container = btn_dl.parent()
		event.preventDefault();
		container.css({ top: event.pageY, left: event.pageX });
		container.show();
		btn_dl.attr('href', "sf/" + v['id']);
	    });
	}
	else {
	    let area = DropArea.build(template, 'file', true, make_cb(template));
	    let cb = target => {
		if (template && target && template.get(0) === target.get(0)) {
		    return false;
		}
		return true;
	    };
	    area.hide_placeholder();
	    area.set_hover_cb(cb);
	    template.find(".hidebutton").click(event => {
		template.find(".hideable").first().toggle()
		if (collapsed_folders.indexOf(template.data('id')) == -1) {
		    collapsed_folders.push(template.data('id'));
		}
		else {
		    collapsed_folders = collapsed_folders.filter(x => { return x != template.data('id'); });
		}
		event.stopPropagation();
	    });
	}
	template.on("drag", function(event) {
	    event.stopPropagation();
	    DropArea.set_target($(this));
	});
	let files = v['content'].filter(x => { return x['mode'] == 'file' });
	let dirs  = v['content'].filter(x => { return x['mode'] != 'file' });
	let ls = template.find('.content > .list').first()
	files.sort((a,b) => { return (a['name'].toLowerCase() > b['name'].toLowerCase() ? 1 : -1)});
	dirs.sort((a,b) => { return (a['name'].toLowerCase() > b['name'].toLowerCase() ? 1 : -1)});
	$.each(dirs, (i, v) => { ls.append(_item(i, v)); });
	$.each(files, (i, v) => { ls.append(_item(i, v)); });
	if (ls.children().length > 0) {
	    ls.removeAttr('style');
	}
	return template;
    }

    let filelist = $("#filelist");
    let root = { name: '/', content: data, 'mode': 'directory' }
    filelist.empty();
    filelist.append(_item(0, root));
    //$.each(data, (i, v) => { return filelist.append(_item(i, v)); });
    filelist.find(".idms_file").each((i, v) => {
	let val = $(v);
	if (collapsed_folders.indexOf(val.data('id')) > -1) {
	    val.find(".hideable").first().hide();
	}
    });
    show_filebrowser(xhr, status);
}

function create_folder() {
    let payload = { name: $("#newfoldername").val() }
    $("#filebrowser").show();
    $.ajax({
	url: 'dir',
	type: 'POST',
	data: JSON.stringify(payload),
	dataType: 'json',
	contentType: false,
	processData: false,
	error: show_filebrowser,
	success: create_filebrowser,
    });
}

function populate_active(data, status, xhr) {
    function render(policy) {
	let icon = (policy['status'] == "BAD" ? "red times circle" : (policy['status'] == "INACTIVE" ? "grey question circle" : "green check circle"));
	let title = (policy['status'] == "BAD" ? "Failed" : (policy['status'] == "INACTIVE" ? "No matching files" : "Valid"));
	template = $(`
          <div class="item"><i class="large ${icon} icon" title="${title}"></i>
            <div class="content">
              <div class="header">${policy['id']}</div>
              <div class="description"></div>
          </div>
        `);
	return template;
    }
    $("#activelist").empty()
    $.each(data, (i, v) => { $("#activelist").append(render(v)); });
}

policies.onreadystatechange = function() {
    if (this.readyState == 4 && this.status == 200) {
	models = JSON.parse(this.responseText);
	fill_toolbox();
    }
}

policies.open("GET", "r", true);
policies.send();

$(window).ready(function() {
    let div = document.createElement('div');
    let msg = false;
    if (!(('draggable' in div) || ('ondragstart' in div && 'ondrop' in div))) {
	$("#modalmsg").append('<div style="padding: 5px;"><i class="exclamation triangle icon"></i><b>This browser does not support Drag and Drop</b><br/> Policy construction will be unavailable</div>');
	$("#toolbox").hide();
	$("#pending").hide();
	msg = true;
    }
    if (!'FileReader' in window) {
	$("#modalmsg").append('<div style="padding: 5px;"><i class="exclamation triangle icon"></i><b>This broswer does not support File Drag and Drop</b><br/> Folders will be unavailable</div>');
	$("#filedrop").hide();
	msg = true;
    }
    if (msg) { $(".ui.modal.warning").modal('show'); }
    $("#addfolder").click(event => {
	$(".ui.modal.folder input[type='text']").focus();
	$(".ui.modal.folder").modal({ onApprove: create_folder }).modal('show');
    });
    $(".ui.modal.folder input[type='text']").keypress(event => {
	if (event.which == 13) {  $(".ui.modal.folder .approve").click() }
    });
    $(".idms_match").on("drag", function(event) {DropArea.set_target($(this))});
    pending_drop = DropArea.build($("#pending-box"), 'policy', false, make_pending_policy);
    let filebrowser = DropArea.build($("#idms_files"), 'file', true);

    $.get({
	url: 'f',
	error: show_filebrowser,
	success: create_filebrowser
    });
    
    $("#uploadbutton").on('change', manual_upload);
    setInterval(() => {
	$.get({
	    url: 'a',
	    success: populate_active
	});
    }, 1000);
});
