#Run this to delete the out directories and recreate them. 
#usage examples:
# the first time you run the code you need to set up the out directory

unlink("out", recursive=TRUE)

dir.create("out")
#dir.create("out/data")
dir.create("out/tabs")
dir.create("out/reps")
dir.create("out/figs")

