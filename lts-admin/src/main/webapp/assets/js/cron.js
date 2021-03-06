function everyTime(dom) {
    var item = $("input[name=v_" + dom.name + "]");
    item.val("*");
    item.change();
}
function unAppoint(dom) {
    var name = dom.name;
    var val = "?";
    if (name == "year") val = "";
    var item = $("input[name=v_" + name + "]");
    item.val(val);
    changeOthers(name);
    item.change();
}
function appoint(dom) {}
function cycle(dom) {
    var name = dom.name;
    var ns = $(dom).parent().find(".numberspinner");
    var start = ns.eq(0).numberspinner("getValue");
    var end = ns.eq(1).numberspinner("getValue");
    var item = $("input[name=v_" + name + "]");
    item.val(start + "-" + end);
    changeOthers(name);
    item.change();
}

function startOn(dom) {
    var name = dom.name;
    var ns = $(dom).parent().find(".numberspinner");
    var start = ns.eq(0).numberspinner("getValue");
    var end = ns.eq(1).numberspinner("getValue");
    var item = $("input[name=v_" + name + "]");
    item.val(start + "/" + end);
    changeOthers(name);
    item.change();
}

function changeOthers(name) {
	if(name === 'min') {
		var second = $("input[name=v_second]").val();
		if(second == '*'){
			$("input[name=v_second]").val("0");
		}
	}	
	
	if(name === 'hour') {
		var second = $("input[name=v_second]").val();
		if(second == '*'){
			$("input[name=v_second]").val("0");
		}
		
		var minute = $("input[name=v_min]").val();
		if(minute == '*'){
			$("input[name=v_min]").val("0");
		}
	}
	
	if(name === 'day') {
		var second = $("input[name=v_second]").val();
		if(second == '*'){
			$("input[name=v_second]").val("0");
		}
		
		var minute = $("input[name=v_min]").val();
		if(minute == '*'){
			$("input[name=v_min]").val("0");
		}
		
		var hour = $("input[name=v_hour]").val();
		if(hour == '*'){
			$("input[name=v_hour]").val("0");
		}		
	}
	
	if(name === 'mouth') {
		var second = $("input[name=v_second]").val();
		if(second == '*'){
			$("input[name=v_second]").val("0");
		}
		
		var minute = $("input[name=v_min]").val();
		if(minute == '*'){
			$("input[name=v_min]").val("0");
		}
		
		var hour = $("input[name=v_hour]").val();
		if(hour == '*'){
			$("input[name=v_hour]").val("0");
		}
		
		var day = $("input[name=v_day]").val();
		if(day == '*'){
			$("input[name=v_day]").val("1");
		}			
	}	
	
	if(name === 'week') {
		var second = $("input[name=v_second]").val();
		if(second == '*'){
			$("input[name=v_second]").val("0");
		}
		
		var minute = $("input[name=v_min]").val();
		if(minute == '*'){
			$("input[name=v_min]").val("0");
		}
		
		var hour = $("input[name=v_hour]").val();
		if(hour == '*'){
			$("input[name=v_hour]").val("0");
		}
		
		var day = $("input[name=v_day]").val();
		if(day == '*'){
			$("input[name=v_day]").val("1");
		}
		
		var mouth = $("input[name=v_mouth]").val();
		if(mouth == '*'){
			$("input[name=v_mouth]").val("1");
		}		
	}	
	
	if(name === 'year') {
		var second = $("input[name=v_second]").val();
		if(second == '*'){
			$("input[name=v_second]").val("0");
		}
		
		var minute = $("input[name=v_min]").val();
		if(minute == '*'){
			$("input[name=v_min]").val("0");
		}
		
		var hour = $("input[name=v_hour]").val();
		if(hour == '*'){
			$("input[name=v_hour]").val("0");
		}
		
		var day = $("input[name=v_day]").val();
		if(day == '*'){
			$("input[name=v_day]").val("1");
		}
		
		var mouth = $("input[name=v_mouth]").val();
		if(mouth == '*'){
			$("input[name=v_mouth]").val("1");
		}	
		
		var week = $("input[name=v_week]").val();
		if(week == '*'){
			$("input[name=v_week]").val("1");
		}			
	}	
}

function lastDay(dom) {
    var item = $("input[name=v_" + dom.name + "]");
    item.val("L");
    changeOthers(dom.name);
    item.change();
}
function weekOfDay(dom) {
    var name = dom.name;
    var ns = $(dom).parent().find(".numberspinner");
    var start = ns.eq(0).numberspinner("getValue");
    var end = ns.eq(1).numberspinner("getValue");
    var item = $("input[name=v_" + name + "]");
    item.val(start + "#" + end);
    changeOthers(name);
    item.change();
}
function lastWeek(dom) {
    var item = $("input[name=v_" + dom.name + "]");
    var ns = $(dom).parent().find(".numberspinner");
    var start = ns.eq(0).numberspinner("getValue");
    item.val(start + "L");
    changeOthers(dom.name);
    item.change();
}
function workDay(dom) {
    var name = dom.name;
    var ns = $(dom).parent().find(".numberspinner");
    var start = ns.eq(0).numberspinner("getValue");
    var item = $("input[name=v_" + name + "]");
    item.val(start + "W");
    changeOthers(name);
    item.change();
}

$(function() {
    $(".numberspinner").numberspinner({
        onChange: function() {
            $(this).closest("div.line").children().eq(0).click();
        }
    });
    var vals = $("input[name^='v_']");
    var cron = $("#cron");
    vals.change(function() {
        var item = [];
        vals.each(function() {
            item.push(this.value);
        });
        cron.val(item.join(" "));
    });
    
    
    var secondList = $(".secondList").children();
    $("#sencond_appoint").click(function() {
        if (this.checked) {
            secondList.eq(0).change();
        }
    });
    secondList.change(function() {
        var sencond_appoint = $("#sencond_appoint").prop("checked");
        if (sencond_appoint) {
            var vals = [];
            secondList.each(function() {
                if (this.checked) {
                    vals.push(this.value);
                }
            });
            var val = "?";
            if (vals.length > 0 && vals.length < 59) {
                val = vals.join(",");
            } else if (vals.length == 59) {
                val = "*";
            }
            var item = $("input[name=v_second]");
            item.val(val);
            item.change();
        }
    });
    
    
    var minList = $(".minList").children();
    $("#min_appoint").click(function() {
        if (this.checked) {
            minList.eq(0).change();
        }
    });
    minList.change(function() {
        var min_appoint = $("#min_appoint").prop("checked");
        if (min_appoint) {
            var vals = [];
            minList.each(function() {
                if (this.checked) {
                    vals.push(this.value);
                }
            });
            var val = "?";
            if (vals.length > 0 && vals.length < 59) {
                val = vals.join(",");
            } else if (vals.length == 59) {
                val = "*";
            }
            var item = $("input[name=v_min]");
            item.val(val);
            changeOthers("min");
            item.change();
        }
    });
    
    
    var hourList = $(".hourList").children();
    $("#hour_appoint").click(function() {
        if (this.checked) {
            hourList.eq(0).change();
        }
    });
    hourList.change(function() {
        var hour_appoint = $("#hour_appoint").prop("checked");
        if (hour_appoint) {
            var vals = [];
            hourList.each(function() {
                if (this.checked) {
                    vals.push(this.value);
                }
            });
            var val = "?";
            if (vals.length > 0 && vals.length < 24) {
                val = vals.join(",");
            } else if (vals.length == 24) {
                val = "*";
            }
            var item = $("input[name=v_hour]");
            item.val(val);
            changeOthers("hour");
            item.change();
        }
    });
    
    
    var dayList = $(".dayList").children();
    $("#day_appoint").click(function() {
        if (this.checked) {
            dayList.eq(0).change();
        }
    });
    dayList.change(function() {
        var day_appoint = $("#day_appoint").prop("checked");
        if (day_appoint) {
            var vals = [];
            dayList.each(function() {
                if (this.checked) {
                    vals.push(this.value);
                }
            });
            var val = "?";
            if (vals.length > 0 && vals.length < 31) {
                val = vals.join(",");
            } else if (vals.length == 31) {
                val = "*";
            }
            var item = $("input[name=v_day]");
            item.val(val);
            changeOthers("day");
            item.change();
        }
    });
    
    
    var mouthList = $(".mouthList").children();
    $("#mouth_appoint").click(function() {
        if (this.checked) {
            mouthList.eq(0).change();
        }
    });
    mouthList.change(function() {
        var mouth_appoint = $("#mouth_appoint").prop("checked");
        if (mouth_appoint) {
            var vals = [];
            mouthList.each(function() {
                if (this.checked) {
                    vals.push(this.value);
                }
            });
            var val = "?";
            if (vals.length > 0 && vals.length < 12) {
                val = vals.join(",");
            } else if (vals.length == 12) {
                val = "*";
            }
            var item = $("input[name=v_mouth]");
            item.val(val);
            changeOthers("mouth");
            item.change();
        }
    });
    
    
    var weekList = $(".weekList").children();
    $("#week_appoint").click(function() {
        if (this.checked) {
            weekList.eq(0).change();
        }
    });
    weekList.change(function() {
        var week_appoint = $("#week_appoint").prop("checked");
        if (week_appoint) {
            var vals = [];
            weekList.each(function() {
                if (this.checked) {
                    vals.push(this.value);
                }
            });
            var val = "?";
            if (vals.length > 0 && vals.length < 7) {
                val = vals.join(",");
            } else if (vals.length == 7) {
                val = "*";
            }
            var item = $("input[name=v_week]");
            item.val(val);
            changeOthers("week");
            item.change();
        }
    });
    
});