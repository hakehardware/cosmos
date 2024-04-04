from textual.screen import Screen
from textual.widgets import Footer, Header
from textual.app import ComposeResult

class SplashScreen(Screen):

    def compose(self) -> ComposeResult:
        yield Header()
        yield Footer()