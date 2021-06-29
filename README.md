# How to setup 

## From requirements.txt

1. ` python -m venv venv `
2. ` source venv/bin/activate `
3. ` pip install -r requirements.txt `
4.  
    - ` PYTHONPATH="." python examples/<EXAMPLE-FILE>.py ` OR 
    - ` export PYTHONPATH="."; python examples/<EXAMPLE-FILE>.py `

## Using Wheel 

1. `python -m venv venv` 
2. `source venv/bin/activate` 
3. `pip install <path-to-wheel>`
4. `python examples/<EXAMPLE-FILE>.py `