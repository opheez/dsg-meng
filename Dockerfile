FROM mcr.microsoft.com/dotnet/sdk:7.0

# Install LLDB
RUN apt-get update && apt-get install -y lldb
RUN apt-get install -y procps
RUN apt-get install -y sudo

# Install .NET diagnostic tools
RUN dotnet tool install --global dotnet-sos
RUN dotnet tool install --global dotnet-dump
RUN dotnet tool install --global dotnet-trace

# Set path for global tools
ENV PATH="${PATH}:/root/.dotnet/tools"

# Automatically configure SOS for LLDB
RUN dotnet-sos install

WORKDIR /app

# Copy Workload Files
COPY ./cs/research/darq/workloads ./workloads

# Build Travel Reservation
COPY ./cs .
WORKDIR /app/research/darq/TravelReservation
RUN dotnet restore
RUN dotnet build -c Debug -o out /p:DebugType=portable


EXPOSE 4022
USER root