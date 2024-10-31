from flask_app import app
from flask_app.models import user
from flask import render_template, redirect, request, session
app.secret_key = "Bobby Newport"
from flask_bcrypt import Bcrypt
bcrypt = Bcrypt(app)

@app.route('/')
def root():
    return render_template('login.html', user = user.User)


@app.route('/register', methods = ["POST"])
def register():
    if not user.User.validate_user(request.form):
        return redirect('/')
    data = {
        "first_name" : request.form["first_name"],
        "last_name" : request.form["last_name"],
        "email" : request.form["email"],
        "password" :  bcrypt.generate_password_hash(request.form['password'])
    }
    session["user_id"] = user.User.register(data)
    return redirect('/dashboard')

@app.route('/login', methods = ["POST"])
def login():
    if not user.User.validate_login(request.form):
        return redirect('/')
    data = {
        "email" : request.form["email"],
        "password" : request.form["password"]
    }
    current_user = user.User.get_email(data)
    session["user_id"] = current_user.id
    return redirect('/dashboard')

@app.route('/logout')
def logout():
    session.clear()
    return redirect('/')