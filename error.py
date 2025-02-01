from fastapi import HTTPException, status
from fastapi.exceptions import RequestValidationError
from sqlglot.errors import ParseError
import functools

def error_handler(func):
    """
    A decorator to handle common SQL parsing and validation errors,
    including unsupported dialects and Pydantic validation errors.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)  # Execute the original function
        except RequestValidationError as ve:
            # Catch Pydantic validation errors and return a cleaner message
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Invalid request: " + ", ".join([f"{error['msg']}" for error in ve.errors()])
            )
        except ParseError as e:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Failed to parse SQL: {e}"
            )
        except ValueError as ve:
            if "Unknown dialect" in str(ve):
                raise HTTPException(
                    status_code=status.HTTP_406_NOT_ACCEPTABLE,
                    detail=f"Unsupported SQL dialect: {ve}"
                )
            raise HTTPException(
                status_code=status.HTTP_406_NOT_ACCEPTABLE,
                detail=str(ve)
            )
        except Exception as ex:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"An unexpected error occurred: {ex}"
            )
    return wrapper
