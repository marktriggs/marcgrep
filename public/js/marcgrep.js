function reset_form(elt) {
  $(elt).find(':text').val('');
}

function remove_groups() {
  var checked = $('.grouper_checkbox:checked');

  checked.each(function (idx, elt) {
      $(elt).parents('.clause_group').remove();
    });

  if ($('.clause_group').size() == 0) {
    location.reload(true);
  } else {
    update_form();
  }
}


function clone_clause(clause)
{
  var cloned = $(clause).clone(true);
  $(cloned).find('select.boolean_options').val($(clause).find('select.boolean_options').val());
  $(cloned).find('select.operator').val($(clause).find('select.operator').val());

  return cloned;
}

function merge_groups() {
  var checked = $('.grouper_checkbox:checked');

  if (checked.size() < 2) {
    return;
  }

  checked.attr('checked', false);

  var borg = $(checked[0]).parents('.clause_group');

  checked.each(function (idx, victim_checkbox) {
      if (idx > 0) {
        var victim = $(victim_checkbox).parents('.clause_group');
        var reborn = victim.clone(true);

        victim.fadeOut(500, function () {
            victim.find('.where_clause').each(function (idx, clause) {
                borg.append(clone_clause(clause));
              });
            borg.css('border-right', 'solid blue 4px');
            victim.remove();
            update_form();
          });
      }
    });

}


function split_groups() {
  var checked = $('.grouper_checkbox:checked');

  var groups_to_split = []
    checked.each(function (idx, elt) {
        var group = $(elt).parents('.clause_group');
        var clauses = $(group).find('.where_clause');

        if (clauses.size() > 1) {
          groups_to_split.push(group);
        }
      });

  $(groups_to_split).each(function (idx, group) {
      var clauses = $(group).find('.where_clause');

      $(clauses.get().reverse()).each(function (idx, clause) {
          insert_clause(clone_clause(clause), group);
        });

      group.remove();
    });

  update_form();
}


function update_buttons() {
  var checked = $('.grouper_checkbox:checked');

  if (checked.size() == 0) {
    $('.remove_button').fadeOut();
    $('.ungroup_button').fadeOut();
  } else {
    $('.remove_button').fadeIn();
  }

  checked.each(function (idx, elt) {
      var group = $(elt).parents('.clause_group');
      if ($(group).find('.where_clause').size() > 1) {
        $('.ungroup_button').fadeIn();
        return;
      }
    })

    if (checked.size() > 1) {
      $('.group_button').fadeIn();
    } else {
      $('.group_button').fadeOut();
    }
}


function update_form() {
  $('.grouper_checkbox').remove();

  $('.clause_group:first').find('.boolean_label:first').html('<b>Where</b>');

  $('.where_clause').each(function (idx, clause) {
      var selected = $(clause).find('.operator option:selected');
      $(clause).find('.field_value').attr('disabled', !!selected.attr('no_value'));
    });

  var groups = $('.clause_group');

  groups.each(function (idx, group) {
      var checkbox = $('<input class="grouper_checkbox" type="checkbox" />');
      checkbox.click(function (checkbox) {
          update_buttons();
        });
      $(group).prepend(checkbox);
    });

  update_buttons();
  if (generate_query()) {
    $('.querysection').fadeIn();
    $('.submit_button').fadeIn();
  } else {
    $('.submit_button').fadeOut();
    $('.querysection').fadeOut();
  }
}


function add_clause(elt, boolean_op) {
  var my_clause = $('.where_clause:last');

  var new_clause = my_clause.clone(true);

  reset_form(new_clause);

  insert_clause(new_clause, my_clause.parents('.clause_group'), boolean_op);
}


function insert_clause(elt, after_elt) {
  var boolean_op = arguments[2];
  var elt = $(elt);

  if (boolean_op) {
    elt.find('.boolean_label').empty();

    var options = $('.boolean_options:first').clone(true);
    options.val(boolean_op);
    options.show();
    elt.children('.boolean_label').append(options);
  }

  after_elt.after(elt);

  $(elt).wrap('<div class="clause_group" />');

  update_form();
}


function clause_to_obj(clause) {
  var value = false;
  var valueNode = $(clause).find('.field_value');
  if (!valueNode.attr('disabled')) {
    value = valueNode.val();
  }

  subquery = {"operator" : $(clause).find('.operator').val(),
              "field" : $(clause).find('.target_field:first').val(),
              "value" : value};

  if (subquery['field']) {
    return subquery;
  } else {
    return false;
  }
}


function clause_boolean(clause) {
  var label = $(clause).find('.boolean_label:first > select');

  if (label.size() != 0) {
    return label.val();
  } else {
    return false;
  }
}


function generate_query() {
  var query = false;

  $('.clause_group').each(function (idx, clause_group) {
      var subquery = false;

      $(clause_group).find('.where_clause').each(function (idx, clause) {
          if (!subquery) {
            subquery = clause_to_obj(clause);
          } else {
            var newquery = clause_to_obj(clause);

            if (newquery) {
              subquery = {"boolean" : clause_boolean(clause),
                          "left" : subquery,
                          "right" : newquery};
            }
          }
        });

      if (query) {
        if (subquery) {
          query = {"boolean" : $(clause_group).find('.boolean_label:first > select').val(),
                   "left" : query,
                   "right" : subquery};
        }
      } else {
        query = subquery;
      }

    });

  if (query) {
    var query_text = JSON.stringify(query, undefined, 4);
    $('#querydisplay').text(query_text);

    return query_text;
  } else {
    return false;
  }
}


function get_job_list()
{
  $.ajax({
      "type" : "GET",
        "url" : "/job_list",
        "success" : function(data) {
        var list = $('#job_list');

        $('.job').remove();

        $(data['jobs']).each(function (idx, job) {
            var row = $('<tr class="job"></tr>');
            row.append('<td><pre>' + JSON.stringify(job['query'], undefined, 4) + '</pre></td>');
            row.append('<td>' + job['time'] + '</td>');

            var status = job['status'];

            if (job['records-checked'] > 0) {
              status += ' (' + job['hits'] + ' out of ' + job['records-checked'] + ' checked)';
            }

            row.append('<td>' + status + '</td>');

            if (job['file-available']) {
              row.append('<td><a href="/job_output/' + job['id'] + '">Download output</a></td>');
            }

            var delete_button = $('<input class="delete_job" type="button" value="delete"/>');
            delete_button.data('job_id', job['id']);
            row.append(delete_button);
            $(delete_button).wrap('<td />');

            list.append(row);
          });

        $('.delete_job').click(function (event) {
            var job_id = $(event.target).data('job_id');
            $.ajax({
                "type" : "POST",
                  "url" : "/delete_job",
                  "data" : {"id" : job_id},
                  "success" : function (data) {
                    get_job_list();
                  }});
          });

        if ($('.job').size() > 0) {
          $('.joblist').fadeIn();
        } else {
          $('.joblist').fadeOut();
        }
      },
        });
}


function run_jobs()
{
  $.ajax({
      "type" : "POST",
        "url" : "/run_jobs",
        "success" : function (data) {
          get_job_list();
        }});
}
