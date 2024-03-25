FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Copy csproj and restore any dependencies (via NuGet)
COPY .cs .
WORKDIR /app/research/darq/TravelReservation
RUN dotnet restore

# Copy the project files and build our release
RUN dotnet publish -c Release -o out

# Generate the runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app
COPY --from=build-env /app/research/darq/TravelReservation/out ./TravelReservation