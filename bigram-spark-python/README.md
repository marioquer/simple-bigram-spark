# instructions
> We've zip our testing env into this package together. If it doesn't work, do the first and second steps to create a new one.
1. create and activate a virtualenv
`python3 -m venv venv && source venv/bin/activate`

2. install dependencies
`pip3 install -r requirements.txt`

3. make sure java version has been switch to 1.8
We don't have permission to do that on Andrew Machine, so maybe TAs need to do this.

4. run program
The first parameter is the input file path.
```
# run wordcount
python3 wordcount.py files/const.txt

# run bigram
python3 bigram.py files/const.txt
```