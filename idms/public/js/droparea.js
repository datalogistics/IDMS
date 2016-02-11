var DropArea = DropArea || (() => {
    let droptarget = null;
    function build(target, type, list, cb, place) {
	if (typeof list === 'undefined') list = false;
	if (typeof cb === 'undefined') cb = (s,v) => { return v; }
	if (typeof place === 'undefined') place = target;

	let template = place.find(".template > *").first();
	let self = false;
	let children = [];
	let placeholder_hidden = false;
	let over_cb = null;
	let dom = $(`
          <div class="droparea idms_thaw">
            <div class="freezer">
              <div class="dropcontainer"></div>
              <div class="dropplaceholder"></div>
            </div>
          </div>`);

	let container = dom.children('.freezer').children('.dropcontainer');
	let placeholder = dom.children('.freezer').children('.dropplaceholder');
	if (template.length > 0) {
	    placeholder.append(template);
	}
	place.append(dom);

	let reset = () => {
	    container.empty();
	    placeholder.show();
	    placeholder.removeClass("idms_drag");
	    container.find(".droparea").remove();
	    for (child in children) {
		children[child].reset();
		children[child].remove();
	    }
	};

	let attach_child = child => {
	    children.push(child);
	};

	let remove = () => {
	    target.unbind("dragover");
	    target.off("drop");
	    target.unbind("dragleave");
	    dom.remove()
	};

	let set_hover_cb = cb => {
	    over_cb = cb
	}

	let hide_placeholder = () => {
	    placeholder_hidden = true;
	    placeholder.hide();
	};

	let over = event => {
	    event.preventDefault();
	    if (over_cb && !over_cb(droptarget)) {
		return;
	    }
	    let data = event.originalEvent.dataTransfer;
	    if ((droptarget && droptarget.data('type').split(' ').indexOf(type) > -1) ||
		(type == 'file' && data && !droptarget)) {
		if (list) { placeholder.show(); }
		placeholder.addClass("idms_drag");
		event.stopPropagation();
	    }
	};
	let drop = event => {
	    event.preventDefault();
	    let data = event.originalEvent.dataTransfer;
	    if ((droptarget && droptarget.data('type').split(' ').indexOf(type) == -1) &&
		(!data || data.files.length < 1) ||
		(!list && !placeholder.is(':visible'))) {
		return;
	    }
	    if (droptarget && droptarget.data('type').split(' ').indexOf(type) > -1) {
		if (placeholder.hasClass("idms_drag")) {
		    let template = droptarget.find(".template > *").first();
		    placeholder.removeClass("idms_drag");
		    placeholder.hide();
		    container.append(cb(self, template.clone(), type));
		    event.stopPropagation();
		}
	    }
	    else if (data && data.files[0]) {
		placeholder.removeClass("idms_drag");
		$("#filebrowser").show();
		let payload = new FormData();
		let parent = target.data('id');
		if ( parent == "undefined") { parent = ""; }
		if (placeholder_hidden) { placeholder.hide(); }
		payload.append('parent', parent);
		event.stopPropagation();
		$.each(data.files, (i, f) => { payload.append('files', f); });
		$.ajax({
		    url: 'f',
		    type: 'POST',
		    data: payload,
		    dataType: 'json',
		    cache: false,
		    contentType: false,
		    processData: false,
		    error: show_filebrowser,
		    success: create_filebrowser,
		});
	    }
	}
	let leave = event => {
	    event.preventDefault();
	    if (list && container.children().length > 0) { placeholder.hide(); }
	    placeholder.removeClass("idms_drag");
	    if (placeholder_hidden) {
		placeholder.hide();
	    }
	}

	target.bind("dragover", over);
	target.on("drop", drop);
	target.bind("dragleave", leave);
	target.on("dragenter", event => { event.preventDefault(); });

	self = { reset: reset, attach_child: attach_child,
		 remove: remove, hide_placeholder: hide_placeholder,
		 set_hover_cb: set_hover_cb }
	return self;
    }

    function set_target(target) {
	droptarget = target;
    }

    function get_target() {
	return droptarget;
    }

    return { build: build, set_target: set_target, get_target: get_target }
})();

$(window).ready(function() {
    $(document).on("dragend", function(event) {
	event.preventDefault();
	DropArea.set_target(null);
	$(".freezer").removeClass("idms_freeze");
    });
    $(document).on("dragstart", function(event) {
	event.originalEvent.dataTransfer.setData('text/html', event.target);
    });
    $(document).on("dragover", function(event) {
	$(".freezer").addClass("idms_freeze");
    });
});
