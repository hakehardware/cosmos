from screens.Splash import SplashScreen

from textual.app import App


class Cosmos(App):
    def __init__(self) -> None:
        super().__init__()

    BINDINGS = [
        ("q", "app.quit", "Quit")
    ]

    SCREENS = {
        'splash': SplashScreen()
    }

    def on_mount(self) -> None:
        self.push_screen('splash')

if __name__ == "__main__":
    Cosmos().run()