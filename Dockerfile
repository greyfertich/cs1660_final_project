#FROM anapsix/alpine-java:8_jdk
FROM ubuntu:16.04
WORKDIR usr/src/app
COPY * ./

ENV DISPLAY :0
ENV JAVA_HOME /opt/jdk
ENV PATH ${PATH}:${JAVA_HOME}/bin

#RUN apk update
#/RUN apk add libxext.x86_64
#/RUN apk add libXrender.x86_64
#/RUN apk add libXtst.x86_64
#RUN apk add libxt
#RUN apk add libxext
#RUN apk add libxrender
#RUN apk add libxtst
#RUN apk add libxi
#WORKDIR /usr/src/final_project
#COPY ./lib ./usr/src/
#COPY Gui.java /usr/src/final_project
#RUN javac Gui.java
#CMD ["java","Gui"] 


ENV ACCESS_KEY null
RUN apt-get update && apt-get install -y default-jdk && javac -cp *: Gui.java
#Install the google SDK
#RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
#RUN mkdir -p /usr/local/gcloud
#RUN tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz
#RUN /usr/local/gcloud/google-cloud-sdk/install.sh
#RUN /usr/local/gcloud/google-cloud-sdk/bin/gcloud init
#RUN gcloud auth activate-service-account --key-file=gcp_key.json
CMD ["java", "-cp", "*:", "Gui"]

