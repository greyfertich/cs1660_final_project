# cs1660_final_project

Video link: https://youtu.be/AN-6n1EDaRA

Notes on running the program:

Your google credentials must be placed in the same directory as the code being run, I used a file called gcp_key.json and the constants in the code must be changed as well (discussed in the video).

To create the container I ran
```
sudo docker build -t final .
```
Then to run it:
```
docker run --rm -e DISPLAY=10.95.111.110:0 final
```
