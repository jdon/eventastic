
if ! nc -vz 0.0.0.0 5432; then
     docker run -d 	-p 5432:5432 -e POSTGRES_PASSWORD=password postgres

fi