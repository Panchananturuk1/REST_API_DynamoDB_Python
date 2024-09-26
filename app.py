from flask import Flask, jsonify, request
import boto3
from botocore.exceptions import ClientError

# Initialize Flask app
app = Flask(__name__)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Connect to the DynamoDB table 'users'
table = dynamodb.Table('crud_op')

# Helper function to handle DynamoDB errors
def handle_dynamodb_error(e):
    return jsonify({"error": str(e)}), 500

# API to create a new user
@app.route('/users', methods=['POST'])
def create_user():
    data = request.json
    try:
        response = table.put_item(
            Item={
                'emp_id': data['emp_id'],
                'emp_name': data['emp_name'],
                'doj': data['doj']
            }
        )
        return jsonify({"message": "User created successfully"}), 201
    except ClientError as e:
        return handle_dynamodb_error(e)

# API to read user information by emp_id
@app.route('/users/<emp_id>', methods=['GET'])
def get_user(emp_id):
    try:
        response = table.get_item(
            Key={'emp_id': emp_id}
        )
        item = response.get('Item')
        if item:
            return jsonify(item), 200
        else:
            return jsonify({"message": "User not found"}), 404
    except ClientError as e:
        return handle_dynamodb_error(e)

# API to update an existing user
@app.route('/users/<emp_id>', methods=['PUT'])
def update_user(emp_id):
    data = request.json
    try:
        response = table.update_item(
            Key={'emp_id': emp_id},
            UpdateExpression="set emp_name = :n, doj = :d",
            ExpressionAttributeValues={
                ':n': data['emp_name'],
                ':d': data['doj']
            },
            ReturnValues="UPDATED_NEW"
        )
        return jsonify({"message": "User updated successfully", "updatedAttributes": response['Attributes']}), 200
    except ClientError as e:
        return handle_dynamodb_error(e)

# API to delete a user by emp_id
@app.route('/users/<emp_id>', methods=['DELETE'])
def delete_user(emp_id):
    try:
        response = table.delete_item(
            Key={'emp_id': emp_id}
        )
        return jsonify({"message": "User deleted successfully"}), 200
    except ClientError as e:
        return handle_dynamodb_error(e)

# API to get all users
@app.route('/users', methods=['GET'])
def get_all_users():
    try:
        scan_response = table.scan()
        items = scan_response.get('Items', [])
        return jsonify(items), 200
    except ClientError as e:
        return handle_dynamodb_error(e)

# Run Flask app
if __name__ == '__main__':
    app.run(debug=True)
