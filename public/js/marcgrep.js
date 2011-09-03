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
    $('.preview_query').fadeIn();
    $('.submit_button').fadeIn();
  } else {
    $('.submit_button').fadeOut();
    $('.preview_query').fadeOut();
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
        "url" : "job_list",
        "success" : function(data) {
        var list = $('#job_list');

        $('.job').remove();

        $(data['jobs']).each(function (idx, job) {
            var row = $('<tr class="job"></tr>');
            row.append('<td class="query_column"><div class="job_query">' + JSON.stringify(job['query'], undefined, 4) + '</div></td>');
            row.append('<td>' + job['time'] + '</td>');

            var status = job['status'];

            if (job['records-checked'] > 0) {
              status += ' (' + job['hits'] + ' hits; ' + job['records-checked'] + ' records checked)';
            }

            row.append('<td>' + status + '</td>');

            if (job['file-available']) {
              row.append('<td><a href="job_output/' + job['id'] + '">Download output</a></td>');
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
                  "url" : "delete_job",
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
      }
    });
}


function run_jobs()
{
  $.ajax({
      "type" : "POST",
        "url" : "run_jobs",
        "success" : function (data) {
          get_job_list();
        }});
}


function extract_output_options()
{
  var options = {}
  $('.input_field:visible').each(function (idx, field) {
      options[$(field).attr('name')] = $(field).val();
    });

  return JSON.stringify(options);
}


function text_input(parent_elt, name, class_name, label, caption)
{
    parent_elt.append($('<div class="captioned_input ' + class_name + '_container">' +
                        (label ? '<label for="' + name + '">' + label + '</label>' : '')+
                        '<div style="display: inline-block">' +
                        '<input class="' + class_name + ' input_field" ' +
                        'type="text" name="' + name + '" />' + 
                        '<div class="caption ' + class_name + '_caption">' + caption + '</div>' +
                        '</div>' +
                        '</div>'));
}


function render_field(field)
{
  var result = $('<div class="field_options"></div>');

  if (field['type'] == 'text') {
    text_input(result, field['name'], 'input_field', field['label'], field['caption']);
  }

  result.hide();

  return result;
}


function show_output_options(select)
{
  var option = $(select).find(':selected');
  var elt = option.data('options');

  $('.field_options').hide();

  if (elt) {
    elt.show();
  }
}


function get_output_options()
{
  $.ajax({
      "type" : "GET",
        "url" : "destination_options",
        "success" : function (data) {
        var output_selection = $('<select id="selected_output"></select>');
        $('.output_options').append(output_selection);

        $(data).each(function (idx, option) {
            var option_elt = $('<option value="' + idx + '">' + option['description'] + '</option>');
            output_selection.append(option_elt);

            $(option['required-fields']).each(function (idx, field) {
                var field_elt = render_field(field);
                $('.output_options').append(field_elt);
                option_elt.data('options', field_elt);
              });
          });

        output_selection.change(function (event) { show_output_options(event.target) });
        show_output_options(output_selection);
      }});
}
