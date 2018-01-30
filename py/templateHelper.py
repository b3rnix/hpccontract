from mako.template import Template

def get_template(template_name):
    return Template(filename='./templates/{0}.txt'.format(template_name))

def run_template(template_name, get_data_func):
    mytemplate = get_template(template_name)
    data = get_data_func()
    return mytemplate.render(data=data)