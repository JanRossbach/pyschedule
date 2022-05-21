import PySimpleGUIQt as sg
from scheduler import Schedule

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
    [sg.Text("Output")],
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
            schedule = Schedule(inputstr) # Create the Scheduler
            schedule.draw_conflict_graph("../resources") # Use it to draw the Conflict Graph to a file conflict.gv.png in resource directory

            # Create the Output with the Scheduler in the required Format using f Strings
            lockstatement = ""
            if "l" in inputstr:
                lockstatement = "Eingabe mit Locks, entsprechent " + schedule.adheres_to()
            else:
                lockstatement = "Eingabe ohne Locks"
            viewstatement = "s ist " + ("" if schedule.is_view_serial() else "nicht ") + "sichtserialisierbar"
            conflictstatement = "s ist " + ("" if schedule.is_conflict_serial() else "nicht ") + "konfliktserialisierbar"
            outputstr = f"""
            {lockstatement}
            {viewstatement}
            {conflictstatement}

            CS2PL:
            {schedule.CS2PL()}
            S2PL:
            {schedule.S2PL()}
            C2PL:
            {schedule.C2PL()}

            Äquivalenter Serieller Schedule:
            {schedule.serial()}
            """

            # Update the Window to reflect show the Output
            window["--OUT--"].update(outputstr)
            window["--GRAPH--"].update("../resources/conflict.gv.png") # Set the conflict graph

    window.close()

if __name__=="__main__":
    main()
