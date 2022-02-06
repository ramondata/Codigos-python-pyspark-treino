import pyautogui
import pyperclip
import time

pyautogui.press('win')
agenda = r'C:\Users\ramon\Pictures\agenda.png'
pyperclip.copy(agenda)
pyautogui.hotkey('ctrl', 'v')
pyautogui.press('enter')

