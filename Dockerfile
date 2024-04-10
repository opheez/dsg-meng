FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build-env
WORKDIR /app

# Copy Workload Files
COPY ./cs/research/darq/workloads ./workloads

# Build Travel Reservation
COPY ./cs .
WORKDIR /app/research/darq/TravelReservation
RUN dotnet restore
RUN dotnet publish -c Release -o out

# Build EventProcessing
WORKDIR /app/research/darq/EventProcessing
RUN dotnet restore
RUN dotnet publish -c Release -o out

# Generate the runtime image
FROM mcr.microsoft.com/dotnet/aspnet:7.0
WORKDIR /app
COPY --from=build-env /app/workloads ./workloads
COPY --from=build-env /app/research/darq/TravelReservation/out ./TravelReservation
COPY --from=build-env /app/research/darq/EventProcessing/out ./EventProcessing