FROM mcr.microsoft.comdotnetsdk7.0 AS build-env
WORKDIR app

# Copy csproj and restore any dependencies (via NuGet)
COPY .cs .
WORKDIR appresearchdarqTravelReservation
RUN dotnet restore

# Copy the project files and build our release
RUN dotnet publish -c Release -o out

# Generate the runtime image
FROM mcr.microsoft.comdotnetaspnet7.0
WORKDIR app
COPY --from=build-env appresearchdarqTravelReservationout .TravelReservation