docker rmi cetko24/meng_project
dotnet publish -c Release
docker build --no-cache . -t cetko24/meng_project