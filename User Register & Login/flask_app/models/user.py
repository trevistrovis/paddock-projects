from flask_app.config.mysqlconnection import connectToMySQL
from flask import flash
import re
EMAIL_REGEX = re.compile(r'^[a-zA-Z0-9.+_-]+@[a-zA-Z0-9._-]+\.[a-zA-Z]+$')
from flask_app import app
from flask_app.models import tree
from flask_bcrypt import Bcrypt
bcrypt = Bcrypt(app)

class User:
    
    my_db = "exam_schema"
    
    def __init__(self, data):
        self.id = data["id"]
        self.first_name = data["first_name"]
        self.last_name = data["last_name"]
        self.email = data["email"]
        self.password = data["password"]
        self.created_at = data["created_at"]
        self.updated_at = data["updated_at"]
        self.trees = []
        
    @staticmethod
    def validate_user(info):
        is_valid = True
        if len(info["first_name"]) < 2:
            flash("Invalid first name!", "register")
            is_valid = False
        if len(info["last_name"]) < 2:
            flash("Invalid second name!", "register")
            is_valid = False
        if not EMAIL_REGEX.match(info["email"]):
            flash("Try another email! This one is invalid", "register")
            is_valid = False
        if len(info["password"]) < 8:
            flash("You need a longer password. 8 characters please!", "register")
            is_valid = False
        if info["password"] != info["confirm_password"]:
            flash("Password does not match", "register")
            is_valid = False
        
        return is_valid
    
    @staticmethod
    def validate_login(info):
        is_valid = True
        email_data = {
            "email": info["email"]
        }
        found_user_or_false = User.get_email(email_data)
        if found_user_or_false == False:
            is_valid = False
            flash("Invalid info", "login")
            return is_valid
        if not bcrypt.check_password_hash(found_user_or_false.password, info['password']):
            is_valid = False
            flash("Invalid info", "login")
        
        
        return is_valid
    
    @classmethod
    def get_id(cls, data):
        query = ("SELECT * FROM users WHERE id = %(id)s")
        results = connectToMySQL(cls.my_db).query_db(query,data)
        return cls(results[0])
    
    @classmethod
    def get_email(cls, data):
        query = ("SELECT * FROM users WHERE email = %(email)s")
        result = connectToMySQL(cls.my_db).query_db(query,data)
        if len(result) < 1:
            return False
        return cls(result[0]) 
        
    @classmethod
    def register(cls, data):
        query = "INSERT INTO users (first_name, last_name, email, password) VALUES (%(first_name)s, %(last_name)s, %(email)s, %(password)s)"
        result = connectToMySQL(cls.my_db).query_db(query,data)
        return result
    
    # @classmethod
    # def get_all_trees_by_user(cls,data):
    #     query = "SELECT * FROM users LEFT JOIN trees ON users.id = trees.user_id WHERE users.id = %(id)s"
    #     results = connectToMySQL(cls.my_db).query_db(query,data)
    #     if len(results) < 1:
    #             return None
    #     else:
    #         this_user = cls(results[0]) 
    #         for row in results:
    #             tree_dictionary = {
    #                 "id" : row["trees.id"],
    #                 "species": row["species"],
    #                 "location": row["location"],
    #                 "reason": row["reason"],
    #                 "date_planted": row["date_planted"],
    #                 "created_at": row["created_at"],
    #                 "updated_at": row["updated_at"],
    #             } 
    #             tree_instance = tree.Tree(tree_dictionary)
    #             this_user.trees.append(tree_instance)
    #         return this_user