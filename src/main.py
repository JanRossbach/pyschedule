import PySimpleGUIQt as sg
from scheduler import Scheduler

sg.theme('Dark Blue 3')   # Set the GUI Theme

# Defining the GUI Layout
input_column = [
    [
        sg.Text("Enter a Schedule: "),
        sg.InputText()
     ],
    [
        sg.Button("Start"),
        sg.Button("Quit")
    ]
]

output_column = [
    [sg.Text("Here comes schedule Output:")],
    [sg.Multiline(default_text="",key="--OUT--")],
    [sg.Image("../resources/basic.gv.png",key="--GRAPH--")]
]

layout = [
    [
        sg.Column(input_column),
        sg.VSeperator(),
        sg.Column(output_column),
    ]
]

def main():
    # Create the Window
    window = sg.Window('PySchedule', layout)
    # Event Loop to process "events" and get the "values" of the inputs
    while True:
        event, values = window.read()
        if event == sg.WIN_CLOSED or event == 'Quit': # if user closes window or clicks cancel
            break # Break out of the Loop and close the window
        if event == 'Start':
            inputstr = values[0] # Read the input schedule
            scheduler = Scheduler(inputstr) # Create the Scheduler
            scheduler.draw_conflict_graph("../resources") # Use it to draw the Conflict Graph to a file conflict.gv.png in resource directory

            # Create the Output with the Scheduler in the required Format using f Strings
            lockstatement = "Eingabe mit Locks" if scheduler.had_locks else "Eingabe ohne Locks"
            viewstatement = "s ist " + ("" if scheduler.view_serial() else "nicht ") + "sichtserialisierbar"
            conflictstatement = "s ist " + ("" if scheduler.view_serial() else "nicht ") + "konfliktserialisierbar"
            outputstr = f"""
            {lockstatement}
            {viewstatement}
            {conflictstatement}
            CS2PL:
            {scheduler.CS2PL()}
            S2PL:
            {scheduler.S2PL()}
            C2PL:
            {scheduler.C2PL()}

            Äquivalenter Serieller Schedule:
            {scheduler.serial()}
            """

            # Update the Window to reflect show the Output
            window["--OUT--"].update(outputstr)
            window["--GRAPH--"].update("../resources/conflict.gv.png") # Set the conflict graph

    window.close()

if __name__=="__main__":
    main()
