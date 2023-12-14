# Use the base image with AWS Glue libraries
FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

# Set environment variables
ENV AWS_PROFILE=default \
    DISABLE_SSL=true

# Create a working directory
WORKDIR /home/glue_user/workspace

# Copy your Spark script into the container
COPY src/$SCRIPT_FILE_NAME .

# Expose the required ports
EXPOSE 4040 18080

# Entrypoint command for spark-submit
CMD ["spark-submit", $SCRIPT_FILE_NAME]
