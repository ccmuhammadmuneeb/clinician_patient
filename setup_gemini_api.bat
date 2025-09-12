@echo off
echo Setting up Gemini API Key
echo ==========================
echo.
echo To use Gemini AI in your recommendation system, you need an API key.
echo.
echo Step 1: Get your API key
echo - Go to: https://aistudio.google.com/app/apikey
echo - Sign in with your Google account
echo - Click "Create API Key"
echo - Copy the generated key
echo.
echo Step 2: Set the environment variable
set /p api_key="Paste your API key here: "
echo.
if "%api_key%"=="" (
    echo No API key provided. Exiting.
    pause
    exit /b 1
)

echo Setting environment variable...
setx GEMINI_API_KEY "%api_key%"
echo.
echo ✅ API key has been set!
echo.
echo IMPORTANT: You need to restart your terminal/command prompt for the
echo environment variable to take effect.
echo.
echo After restarting:
echo 1. Navigate back to this folder
echo 2. Run: python -m uvicorn main:app --host 0.0.0.0 --port 8001 --reload
echo.
pause
