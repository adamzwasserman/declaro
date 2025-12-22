Feature: Declaro Model Validation
  As a Python developer using declaro-persistum
  I want ximinez to validate my model usage against Pydantic models
  So that field access and relationships are type-safe

  Background:
    Given a Pydantic model file "models/user.py" with content:
      """
      from uuid import UUID
      from decimal import Decimal
      from typing import Optional

      # Mock FieldInfo for nullable support
      class FieldInfo:
          def __init__(self, nullable=False):
              self.nullable = nullable

      class User:
          __tablename__ = "users"
          model_fields = {
              "id": FieldInfo(),
              "email": FieldInfo(),
              "name": FieldInfo(nullable=True),
              "age": FieldInfo(nullable=True),
          }
          __annotations__ = {
              "id": UUID,
              "email": str,
              "name": str,
              "age": int,
          }

      class Order:
          __tablename__ = "orders"
          model_fields = {
              "id": FieldInfo(),
              "user_id": FieldInfo(),
              "total": FieldInfo(),
              "status": FieldInfo(),
          }
          __annotations__ = {
              "id": UUID,
              "user_id": UUID,
              "total": Decimal,
              "status": str,
          }
      """
    And ximinez is configured with declaro model path "models/"

  Scenario: Valid field access passes
    Given a Python file with content:
      """
      def get_user_email(user: User) -> str:
          types:
              email: str

          email = user["email"]
          return email
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Invalid field access fails
    Given a Python file with content:
      """
      def get_username(user: User) -> str:
          types:
              username: str

          username = user["username"]
          return username
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'User' has no field 'username'"
    And the violation message contains "did you mean 'name'?"

  Scenario: Field type mismatch fails
    Given a Python file with content:
      """
      def get_user_age(user: User) -> str:
          types:
              age: str

          age = user["age"]
          return age
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'age' is 'int', not 'str'"

  Scenario: Multiple model violations trigger off-by-one
    Given a Python file with content:
      """
      def bad_access(user: User) -> str:
          types:
              username: str
              purchases: list[Order]

          username = user["username"]
          purchases = user["purchases"]
          return username
      """
    When ximinez checks the file
    Then 2 violations are reported
    And the output contains "NOBODY expects a model violation!"
    And the output contains "Our CHIEF weapon:"
    And the output contains "Our TWO chief weapons are:"

  Scenario: Query builder field validation
    Given a Python file with content:
      """
      async def find_user(username: str) -> User:
          types:
              result: User

          result = await query.select("users").where(username=username).one()
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "'users' table has no column 'username'"
    And the violation message contains "did you mean 'name'?"

  Scenario: Query builder with valid fields passes
    Given a Python file with content:
      """
      async def find_user(email: str) -> User:
          types:
              result: User

          result = await query.select("users").where(email=email).one()
          return result
      """
    When ximinez checks the file
    Then no violations are reported

  Scenario: Insert with missing required field
    Given a Python file with content:
      """
      async def create_user(name: str) -> User:
          types:
              user_data: dict
              result: User

          user_data = {"name": name}
          result = await query.insert("users", user_data)
          return result
      """
    When ximinez checks the file
    Then 1 violation is reported
    And the violation message contains "missing required field 'email' for insert into 'users'"

  Scenario: Insert with all required fields passes
    Given a Python file with content:
      """
      async def create_user(email: str, name: str) -> User:
          types:
              user_data: dict
              result: User

          user_data = {"email": email, "name": name}
          result = await query.insert("users", user_data)
          return result
      """
    When ximinez checks the file
    Then no violations are reported
