package me.jaypatelbond.popcorn

import android.Manifest
import android.content.Intent
import android.content.pm.PackageManager
import android.os.Build
import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.activity.result.contract.ActivityResultContracts
import androidx.compose.foundation.layout.padding
import androidx.compose.material3.Icon
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
import androidx.compose.material3.NavigationBarItemDefaults
import androidx.compose.material3.Scaffold
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.core.content.ContextCompat
import androidx.navigation.NavGraph.Companion.findStartDestination
import androidx.navigation.compose.currentBackStackEntryAsState
import androidx.navigation.compose.rememberNavController
import dagger.hilt.android.AndroidEntryPoint
import me.jaypatelbond.popcorn.presentation.navigation.PopcornNavHost
import me.jaypatelbond.popcorn.presentation.navigation.Screen
import me.jaypatelbond.popcorn.presentation.navigation.bottomNavItems
import me.jaypatelbond.popcorn.ui.theme.BackgroundBottomBar
import me.jaypatelbond.popcorn.ui.theme.NetflixRed
import me.jaypatelbond.popcorn.ui.theme.PopcornTheme
import me.jaypatelbond.popcorn.ui.theme.TextSecondary

/**
 * Main entry point of the Popcorn app.
 * Uses Hilt for dependency injection and Compose Navigation with bottom bar.
 * Handles deep links from shared movie URLs.
 */
@AndroidEntryPoint
class MainActivity : ComponentActivity() {

    // Permission launcher for notifications (Android 13+)
    private val notificationPermissionLauncher = registerForActivityResult(
        ActivityResultContracts.RequestPermission()
    ) { isGranted ->
        // Permission result - Chucker will work if granted
        // No action needed if denied, Chucker will just not show notifications
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        enableEdgeToEdge()

        // Request notification permission for Android 13+ (for Chucker in debug builds)
        requestNotificationPermissionIfNeeded()

        // Parse deep link movie ID if app was opened via deep link
        val deepLinkMovieId = parseDeepLinkMovieId(intent)

        setContent {
            PopcornTheme {
                MainApp(deepLinkMovieId = deepLinkMovieId)
            }
        }
    }

    /**
     * Request notification permission on Android 13+ (API 33+).
     * Only needed for debug builds with Chucker.
     */
    private fun requestNotificationPermissionIfNeeded() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (ContextCompat.checkSelfPermission(
                    this,
                    Manifest.permission.POST_NOTIFICATIONS
                ) != PackageManager.PERMISSION_GRANTED
            ) {
                notificationPermissionLauncher.launch(Manifest.permission.POST_NOTIFICATIONS)
            }
        }
    }

    /**
     * Parse movie ID from deep link URI.
     * Expected format: popcorn://movie/{movieId}
     */
    private fun parseDeepLinkMovieId(intent: Intent?): Int? {
        if (intent?.action != Intent.ACTION_VIEW) return null
        
        val uri = intent.data ?: return null
        if (uri.scheme != "popcorn" || uri.host != "movie") return null
        
        // Get the movie ID from the path (e.g., /123 -> 123)
        return uri.lastPathSegment?.toIntOrNull()
    }
}

@Composable
fun MainApp(deepLinkMovieId: Int? = null) {
    val navController = rememberNavController()
    val navBackStackEntry by navController.currentBackStackEntryAsState()
    val currentDestination = navBackStackEntry?.destination

    // Handle deep link navigation
    LaunchedEffect(deepLinkMovieId) {
        deepLinkMovieId?.let { movieId ->
            navController.navigate(Screen.MovieDetails.createRoute(movieId)) {
                // Don't add to back stack if we're navigating directly to details
                launchSingleTop = true
            }
        }
    }

    // Determine if bottom bar should be shown (hide on detail screens)
    val showBottomBar = currentDestination?.route in listOf(
        Screen.Home.route,
        Screen.Search.route,
        Screen.Saved.route
    )

    Scaffold(
        bottomBar = {
            if (showBottomBar) {
                BottomNavigationBar(
                    currentRoute = currentDestination?.route,
                    onItemClick = { route ->
                        navController.navigate(route) {
                            // Pop up to the start destination to avoid building a large back stack
                            popUpTo(navController.graph.findStartDestination().id) {
                                saveState = true
                            }
                            // Avoid multiple copies of the same destination
                            launchSingleTop = true
                            // Restore state when reselecting a previously selected item
                            restoreState = true
                        }
                    }
                )
            }
        },
        containerColor = MaterialTheme.colorScheme.background
    ) { paddingValues ->
        PopcornNavHost(
            navController = navController,
            modifier = Modifier.padding(paddingValues)
        )
    }
}

@Composable
private fun BottomNavigationBar(
    currentRoute: String?,
    onItemClick: (String) -> Unit
) {
    NavigationBar(
        containerColor = BackgroundBottomBar,
        contentColor = Color.White
    ) {
        bottomNavItems.forEach { item ->
            val selected = currentRoute == item.route

            NavigationBarItem(
                selected = selected,
                onClick = { onItemClick(item.route) },
                icon = {
                    Icon(
                        imageVector = if (selected) item.selectedIcon else item.unselectedIcon,
                        contentDescription = item.title
                    )
                },
                label = {
                    Text(
                        text = item.title,
                        style = MaterialTheme.typography.labelSmall
                    )
                },
                colors = NavigationBarItemDefaults.colors(
                    selectedIconColor = NetflixRed,
                    selectedTextColor = NetflixRed,
                    unselectedIconColor = TextSecondary,
                    unselectedTextColor = TextSecondary,
                    indicatorColor = Color.Transparent
                )
            )
        }
    }
}