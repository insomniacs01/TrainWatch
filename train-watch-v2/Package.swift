// swift-tools-version: 5.7
import PackageDescription

let package = Package(
    name: "TrainWatchV2",
    platforms: [
        .iOS(.v16),
        .macOS(.v12),
    ],
    products: [
        .library(
            name: "TrainWatchCore",
            targets: ["TrainWatchCore"]
        ),
    ],
    targets: [
        .target(
            name: "TrainWatchCore"
        ),
        .testTarget(
            name: "TrainWatchCoreTests",
            dependencies: ["TrainWatchCore"]
        ),
    ]
)
