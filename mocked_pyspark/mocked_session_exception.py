class MockedSessionException(Exception):
  def __init__(self, message):
    super().__init__(f"[MOCKED SESSION EXCEPTION]: {message}")